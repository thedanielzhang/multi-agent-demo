from __future__ import annotations

import asyncio
import re
from collections import defaultdict
from dataclasses import dataclass

from mafia.config import AgentConfig, AppConfig, ModeProfile
from mafia.context import ContextAssembler, entropy_from_keywords, keyword_sketch, tokenize
from mafia.event_log import EventLog
from mafia.messages import (
    AgentContextSnapshot,
    MafiaGameSnapshot,
    MafiaPrivateState,
    MafiaPublicState,
    AgentTopicSnapshot,
    CandidateRecord,
    CommitmentState,
    ConversationMessage,
    DeliveryReservation,
    LoggedEvent,
    OpenQuestionState,
    ResponseSlotState,
    RoomDiscourseStateSnapshot,
    RoomMetricsSnapshot,
    SenderKind,
    utc_now,
)

_QUESTION_STARTERS = {"what", "why", "how", "when", "where", "who", "which", "should", "could", "would", "do", "does", "did", "can", "is", "are"}
_STRICT_TURN_PHRASES = (
    "one at a time",
    "the floor is yours",
    "go ahead",
    "let's hear from",
    "lets hear from",
    "who wants to go first",
)
_ACCEPTANCE_PHRASES = ("yes", "correct", "locked", "confirmed", "should be", "must", "that's right", "that is right")
_REJECTION_PHRASES = ("no context switching", "already locked", "don't", "dont", "shouldn't", "shouldnt", "must not")
_NORMALIZE_RE = re.compile(r"[^a-z0-9]+")


class RunStateProjection:
    def __init__(self) -> None:
        self.state = "idle"

    def apply(self, logged_event: LoggedEvent) -> None:
        if logged_event.event.subject == "run.event.state.changed":
            self.state = logged_event.event.payload["state"]


class ConversationTimelineProjection:
    def __init__(self) -> None:
        self.messages: list[ConversationMessage] = []
        self._client_ids: set[str] = set()
        self._by_client_message_id: dict[str, ConversationMessage] = {}

    def has_client_message_id(self, client_message_id: str) -> bool:
        return client_message_id in self._client_ids

    def by_client_message_id(self, client_message_id: str) -> ConversationMessage | None:
        message = self._by_client_message_id.get(client_message_id)
        return message.model_copy(deep=True) if message else None

    def apply(self, logged_event: LoggedEvent) -> None:
        if logged_event.event.subject != "conversation.event.message.committed":
            return
        message = ConversationMessage.model_validate(logged_event.event.payload)
        self.messages.append(message)
        self._client_ids.add(message.client_message_id)
        self._by_client_message_id[message.client_message_id] = message


class CandidateBufferProjection:
    def __init__(self) -> None:
        self.buffers: dict[str, list[CandidateRecord]] = defaultdict(list)
        self.versions: dict[str, int] = defaultdict(int)

    def apply(self, logged_event: LoggedEvent) -> None:
        subject = logged_event.event.subject
        payload = logged_event.event.payload
        if subject.endswith(".candidate.buffered") or subject.endswith(".candidate.requeued"):
            candidate = CandidateRecord.model_validate(payload["candidate"] if "candidate" in payload else payload)
            self.buffers[candidate.agent_id].append(candidate)
            self._bump(candidate.agent_id)
        elif subject.endswith(".candidate.discarded") or subject.endswith(".candidate.reserved"):
            candidate_payload = payload.get("candidate", {})
            agent_id = payload.get("agent_id") or candidate_payload.get("agent_id")
            candidate_id = payload.get("candidate_id") or candidate_payload.get("candidate_id")
            if not agent_id or not candidate_id:
                return
            self.buffers[agent_id] = [
                candidate
                for candidate in self.buffers[agent_id]
                if candidate.candidate_id != candidate_id
            ]
            self._bump(agent_id)

    def _bump(self, agent_id: str) -> None:
        self.versions[agent_id] += 1
        for item in self.buffers[agent_id]:
            item.buffer_version = self.versions[agent_id]


class ReservationProjection:
    def __init__(self) -> None:
        self._reservations: dict[str, DeliveryReservation] = {}

    def reservation_for(self, reservation_id: str) -> DeliveryReservation | None:
        reservation = self._reservations.get(reservation_id)
        return reservation.model_copy(deep=True) if reservation else None

    def active_reservations(self) -> list[DeliveryReservation]:
        active = []
        for reservation in self._reservations.values():
            if reservation.state not in {"aborted", "committed", "expired"}:
                active.append(reservation.model_copy(deep=True))
        return active

    def active_reservation_for(self, agent_id: str) -> DeliveryReservation | None:
        for reservation in self._reservations.values():
            if reservation.agent_id != agent_id:
                continue
            if reservation.state in {"aborted", "committed", "expired", "requeued"}:
                continue
            return reservation.model_copy(deep=True)
        return None

    def apply(self, logged_event: LoggedEvent) -> None:
        subject = logged_event.event.subject
        payload = logged_event.event.payload
        if subject.endswith(".candidate.reserved"):
            reservation = DeliveryReservation.model_validate(payload)
            self._reservations[reservation.reservation_id] = reservation
            return
        if subject.endswith(".candidate.requeued"):
            reservation_id = payload.get("reservation_id")
            if reservation_id and reservation_id in self._reservations:
                self._reservations[reservation_id].state = "requeued"
            return
        if subject.endswith(".delivery.scheduled"):
            self._update(payload["reservation_id"], state="scheduled")
            return
        if subject.endswith(".delivery.submitted"):
            self._update(payload["reservation_id"], state="submitted")
            return
        if subject.endswith(".delivery.acked"):
            self._update(
                payload["reservation_id"],
                state="acked",
                message_id=payload.get("message_id"),
            )
            return
        if subject.endswith(".delivery.aborted"):
            self._update(
                payload["reservation_id"],
                state="aborted",
                last_error=payload.get("reason"),
            )
            return
        if subject == "conversation.event.message.committed":
            metadata = payload.get("metadata", {})
            reservation_id = metadata.get("reservation_id")
            if reservation_id:
                self._update(
                    reservation_id,
                    state="committed",
                    message_id=payload.get("message_id"),
                )

    def _update(self, reservation_id: str, **changes) -> None:
        reservation = self._reservations.get(reservation_id)
        if reservation is None:
            return
        current_state = reservation.state
        for key, value in changes.items():
            if key == "state" and current_state == "committed" and value == "acked":
                continue
            setattr(reservation, key, value)


class AgentTopicProjection:
    def __init__(self) -> None:
        self.snapshots: dict[str, AgentTopicSnapshot] = {}

    def apply(self, logged_event: LoggedEvent) -> None:
        if not logged_event.event.subject.startswith("topic.event.") or not logged_event.event.subject.endswith(".snapshot.updated"):
            return
        snapshot = AgentTopicSnapshot.model_validate(logged_event.event.payload)
        self.snapshots[snapshot.agent_id] = snapshot


class MafiaProjection:
    def __init__(self) -> None:
        self.snapshot: MafiaGameSnapshot | None = None

    def public_state(self) -> MafiaPublicState | None:
        if self.snapshot is None:
            return None
        return self.snapshot.public_state()

    def private_state_for(self, participant_id: str) -> MafiaPrivateState | None:
        if self.snapshot is None:
            return None
        return self.snapshot.private_state_for(participant_id)

    def apply(self, logged_event: LoggedEvent) -> None:
        if logged_event.event.subject != "mafia.event.snapshot.updated":
            return
        self.snapshot = MafiaGameSnapshot.model_validate(logged_event.event.payload)


class RoomMetricsProjection:
    def __init__(self) -> None:
        self.snapshot = RoomMetricsSnapshot()

    def apply(self, logged_event: LoggedEvent, timeline: ConversationTimelineProjection) -> None:
        if logged_event.event.subject != "conversation.event.message.committed":
            return
        recent = timeline.messages[-12:]
        speakers = {message.sender_id for message in recent}
        rates: dict[str, float] = {}
        total = len(recent)
        if total:
            for speaker in speakers:
                rates[speaker] = sum(1 for message in recent if message.sender_id == speaker) / total
        self.snapshot = RoomMetricsSnapshot(
            watermark=logged_event.seq,
            active_participant_count=max(1, len(speakers)),
            avg_message_rate=(1.0 / max(1, len(speakers))),
            agent_message_rates=rates,
            recent_entropy=entropy_from_keywords(recent),
            recent_shift_detected=_detect_shift(recent),
            recent_keyword_sketch=keyword_sketch(recent),
            time_since_last_any=0.0,
        )


class RoomDiscourseProjection:
    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self.snapshot = RoomDiscourseStateSnapshot()
        self._agent_ids = [agent.id for agent in config.agents]
        self._goal_tokens = {
            agent.id: set(tokenize(" ".join([agent.display_name, *agent.goals, agent.style_prompt])))
            for agent in config.agents
        }
        self._display_names = {
            agent.id: self._normalize(agent.display_name)
            for agent in config.agents
        }

    def apply(self, logged_event: LoggedEvent, timeline: ConversationTimelineProjection) -> None:
        if logged_event.event.subject != "conversation.event.message.committed":
            return
        self.snapshot = self._recompute(timeline.messages)

    def _recompute(self, messages: list[ConversationMessage]) -> RoomDiscourseStateSnapshot:
        slot = ResponseSlotState()
        open_questions: list[OpenQuestionState] = []
        resolved_ids: list[str] = []
        accepted: list[CommitmentState] = []
        rejected: list[CommitmentState] = []
        last_owner: str | None = None

        for message in messages:
            if message.sender_kind == SenderKind.HUMAN:
                self._resolve_open_questions(open_questions, resolved_ids, message)
                slot = ResponseSlotState()
                slot_owner, slot_reason = self._slot_from_human_message(message, last_owner)
                if slot_owner:
                    slot = ResponseSlotState(
                        active=True,
                        owner_id=slot_owner,
                        reason=slot_reason,
                        source_message_id=message.message_id,
                    )
                    last_owner = slot_owner
                if self._is_question(message.text):
                    open_questions.append(
                        OpenQuestionState(
                            question_id=message.message_id,
                            source_message_id=message.message_id,
                            asker_id=message.sender_id,
                            asker_display_name=message.display_name,
                            target_participant_id=slot_owner if slot_reason == "direct_question" else None,
                            text_excerpt=message.text[:180],
                            keyword_sketch=keyword_sketch([message], limit=6),
                            created_at=message.created_at,
                        )
                    )
                commitment = self._commitment_from_human_message(message)
                if commitment is not None:
                    if commitment.polarity == "accepted":
                        accepted.append(commitment)
                    else:
                        rejected.append(commitment)
            elif slot.active and message.sender_id == slot.owner_id:
                slot = ResponseSlotState()

        unresolved = [question for question in open_questions if not question.resolved]
        resolved = [question for question in open_questions if question.resolved]
        return RoomDiscourseStateSnapshot(
            strict_turn_active=slot.active,
            slot_owner_id=slot.owner_id,
            slot_reason=slot.reason,
            slot_source_message_id=slot.source_message_id,
            open_questions=unresolved,
            resolved_question_ids=resolved_ids,
            resolved_questions=resolved[-8:],
            accepted_commitments=accepted[-8:],
            rejected_commitments=rejected[-8:],
            last_strict_turn_owner_id=last_owner,
        )

    def _normalize(self, text: str | None) -> str:
        return _NORMALIZE_RE.sub(" ", (text or "").casefold()).strip()

    def _is_question(self, text: str) -> bool:
        compact = text.strip().lower()
        if not compact:
            return False
        if "?" in compact:
            return True
        tokens = tokenize(compact)
        return bool(tokens and tokens[0] in _QUESTION_STARTERS)

    def _is_strict_turn_cue(self, text: str) -> bool:
        compact = self._normalize(text)
        return any(phrase in compact for phrase in _STRICT_TURN_PHRASES)

    def _find_named_agent(self, message: ConversationMessage) -> str | None:
        for mention in message.mentions:
            if mention in self._agent_ids:
                return mention
        lowered = self._normalize(message.text)
        for agent_id in self._agent_ids:
            display_name = self._display_names.get(agent_id) or ""
            if display_name and re.search(rf"\b{re.escape(display_name)}\b", lowered):
                return agent_id
            if len(agent_id) >= 3 and re.search(rf"\b{re.escape(self._normalize(agent_id))}\b", lowered):
                return agent_id
        return None

    def _goal_keyword_owner(self, text: str) -> str | None:
        message_tokens = set(tokenize(text))
        best_agent_id: str | None = None
        best_score = 0
        for agent_id in self._agent_ids:
            overlap = len(message_tokens & self._goal_tokens.get(agent_id, set()))
            if overlap > best_score:
                best_score = overlap
                best_agent_id = agent_id
        return best_agent_id if best_score > 0 else None

    def _round_robin_owner(self, last_owner: str | None) -> str | None:
        if not self._agent_ids:
            return None
        if last_owner not in self._agent_ids:
            return self._agent_ids[0]
        index = self._agent_ids.index(last_owner)
        return self._agent_ids[(index + 1) % len(self._agent_ids)]

    def _slot_from_human_message(self, message: ConversationMessage, last_owner: str | None) -> tuple[str | None, str]:
        named_target = self._find_named_agent(message)
        if self._is_question(message.text) and named_target is not None:
            return named_target, "direct_question"
        if self._is_strict_turn_cue(message.text):
            if named_target is not None:
                return named_target, "strict_turn_named"
            owner = self._goal_keyword_owner(message.text) or self._round_robin_owner(last_owner)
            return owner, "strict_turn_unnamed" if owner is not None else "none"
        return None, "none"

    def _has_acceptance_cue(self, text: str) -> bool:
        lowered = text.casefold()
        return any(phrase in lowered for phrase in _ACCEPTANCE_PHRASES)

    def _has_rejection_cue(self, text: str) -> bool:
        lowered = text.casefold()
        if any(phrase in lowered for phrase in _REJECTION_PHRASES):
            return True
        return bool(re.search(r"\b(no|not|never)\b", lowered))

    def _commitment_from_human_message(self, message: ConversationMessage) -> CommitmentState | None:
        text = message.text.strip()
        if not text:
            return None
        polarity: str | None = None
        if self._has_rejection_cue(text):
            polarity = "rejected"
        elif self._has_acceptance_cue(text):
            polarity = "accepted"
        if polarity is None:
            return None
        return CommitmentState(
            source_message_id=message.message_id,
            polarity=polarity,
            keyword_sketch=keyword_sketch([message], limit=6),
            canonical_text=text[:180],
            created_at=message.created_at,
        )

    def _resolve_open_questions(
        self,
        open_questions: list[OpenQuestionState],
        resolved_ids: list[str],
        message: ConversationMessage,
    ) -> None:
        unresolved = [question for question in open_questions if not question.resolved]
        if not unresolved:
            return
        message_tokens = set(tokenize(message.text))
        answer_like = (
            not self._is_question(message.text)
            or self._has_acceptance_cue(message.text)
            or self._has_rejection_cue(message.text)
        )
        if not answer_like:
            return
        for question in reversed(unresolved):
            overlap = len(message_tokens & set(question.keyword_sketch))
            if overlap > 0 or question.asker_id != message.sender_id:
                question.resolved = True
                question.resolved_by_message_id = message.message_id
                resolved_ids.append(question.question_id)
                return
        latest = unresolved[-1]
        latest.resolved = True
        latest.resolved_by_message_id = message.message_id
        resolved_ids.append(latest.question_id)


def _detect_shift(recent: list[ConversationMessage]) -> bool:
    if len(recent) < 4:
        return False
    first = set(keyword_sketch(recent[:-2], limit=3))
    second = set(keyword_sketch(recent[-2:], limit=3))
    if not first or not second:
        return False
    overlap = len(first & second) / max(1, len(first | second))
    return overlap < 0.34


@dataclass
class ProjectionSnapshot:
    watermark: int
    run_state: str
    messages: list[ConversationMessage]
    room_metrics: RoomMetricsSnapshot
    topic_snapshot: AgentTopicSnapshot | None
    buffer: list[CandidateRecord]
    buffer_version: int


class ProjectionRegistry:
    """Single log-driven projection registry for consistent read snapshots."""

    def __init__(self, event_log: EventLog, config: AppConfig) -> None:
        self._event_log = event_log
        self._config = config
        self._timeline = ConversationTimelineProjection()
        self._run_state = RunStateProjection()
        self._buffers = CandidateBufferProjection()
        self._reservations = ReservationProjection()
        self._topics = AgentTopicProjection()
        self._mafia = MafiaProjection()
        self._room_metrics = RoomMetricsProjection()
        self._discourse = RoomDiscourseProjection(config)
        self._watermark = 0
        self._condition = asyncio.Condition()
        self._task: asyncio.Task[None] | None = None
        self._context_assembler = ContextAssembler(config)

    @property
    def watermark(self) -> int:
        return self._watermark

    @property
    def task(self) -> asyncio.Task[None] | None:
        return self._task

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def close(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def wait_until(self, seq: int) -> None:
        async with self._condition:
            while self._watermark < seq:
                await self._condition.wait()

    def run_state(self) -> str:
        return self._run_state.state

    def latest_messages(self) -> list[ConversationMessage]:
        return list(self._timeline.messages)

    def has_client_message_id(self, client_message_id: str) -> bool:
        return self._timeline.has_client_message_id(client_message_id)

    def message_by_client_message_id(self, client_message_id: str) -> ConversationMessage | None:
        return self._timeline.by_client_message_id(client_message_id)

    def agent_view(self, agent: AgentConfig) -> AgentContextSnapshot:
        messages = list(self._timeline.messages)
        topic_snapshot = self._topics.snapshots.get(agent.id)
        room_metrics = self._room_metrics.snapshot.model_copy()
        now = utc_now()
        if messages:
            room_metrics.time_since_last_any = max(0.0, (now - messages[-1].created_at).total_seconds())
        else:
            room_metrics.time_since_last_any = 9999.0
        if self._config.mode == ModeProfile.BASELINE_TIME_TO_TALK:
            recent_messages = messages
        else:
            recent_messages = messages[- self._config.context_for(agent).recent_window_messages :]
        return self._context_assembler.build(
            agent,
            watermark=self._watermark,
            current_time=now,
            recent_messages=recent_messages,
            room_metrics=room_metrics,
            discourse_state=self._discourse.snapshot.model_copy(deep=True),
            topic_snapshot=topic_snapshot.model_copy(deep=True) if topic_snapshot else None,
            buffer_size=len(self._buffers.buffers.get(agent.id, [])),
            buffer_version=self._buffers.versions.get(agent.id, 0),
            run_state=self._run_state.state,
        )

    def buffer_for(self, agent_id: str) -> list[CandidateRecord]:
        return [candidate.model_copy(deep=True) for candidate in self._buffers.buffers.get(agent_id, [])]

    def buffer_version_for(self, agent_id: str) -> int:
        return self._buffers.versions.get(agent_id, 0)

    def reservation_for(self, reservation_id: str) -> DeliveryReservation | None:
        return self._reservations.reservation_for(reservation_id)

    def active_reservations(self) -> list[DeliveryReservation]:
        return self._reservations.active_reservations()

    def active_reservation_for(self, agent_id: str) -> DeliveryReservation | None:
        return self._reservations.active_reservation_for(agent_id)

    def topic_snapshot_for(self, agent_id: str) -> AgentTopicSnapshot | None:
        snapshot = self._topics.snapshots.get(agent_id)
        return snapshot.model_copy(deep=True) if snapshot else None

    def discourse_state(self) -> RoomDiscourseStateSnapshot:
        return self._discourse.snapshot.model_copy(deep=True)

    def mafia_snapshot(self) -> MafiaGameSnapshot | None:
        snapshot = self._mafia.snapshot
        return snapshot.model_copy(deep=True) if snapshot else None

    def mafia_public_state(self) -> MafiaPublicState | None:
        snapshot = self._mafia.public_state()
        return snapshot.model_copy(deep=True) if snapshot else None

    def mafia_private_state_for(self, participant_id: str) -> MafiaPrivateState | None:
        snapshot = self._mafia.private_state_for(participant_id)
        return snapshot.model_copy(deep=True) if snapshot else None

    async def _run(self) -> None:
        seq = 0
        while True:
            events = await self._event_log.wait_for_events(seq)
            for logged_event in events:
                self._run_state.apply(logged_event)
                self._timeline.apply(logged_event)
                self._buffers.apply(logged_event)
                self._reservations.apply(logged_event)
                self._topics.apply(logged_event)
                self._mafia.apply(logged_event)
                self._room_metrics.apply(logged_event, self._timeline)
                self._discourse.apply(logged_event, self._timeline)
                self._watermark = logged_event.seq
                seq = logged_event.seq
            async with self._condition:
                self._condition.notify_all()
