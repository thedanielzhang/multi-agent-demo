from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass

from mafia.config import AgentConfig, AppConfig, ModeProfile
from mafia.context import ContextAssembler, entropy_from_keywords, keyword_sketch
from mafia.event_log import EventLog
from mafia.messages import (
    AgentContextSnapshot,
    AgentTopicSnapshot,
    CandidateRecord,
    ConversationMessage,
    DeliveryReservation,
    LoggedEvent,
    RoomMetricsSnapshot,
    utc_now,
)


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
        if ".snapshot.updated" not in logged_event.event.subject:
            return
        snapshot = AgentTopicSnapshot.model_validate(logged_event.event.payload)
        self.snapshots[snapshot.agent_id] = snapshot


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
        self._room_metrics = RoomMetricsProjection()
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
                self._room_metrics.apply(logged_event, self._timeline)
                self._watermark = logged_event.seq
                seq = logged_event.seq
            async with self._condition:
                self._condition.notify_all()
