from __future__ import annotations

import re
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator, model_validator


_NON_ALNUM_RE = re.compile(r"[^a-z0-9]+")


def _slugify_topic(value: str) -> str:
    text = _NON_ALNUM_RE.sub("-", value.lower()).strip("-")
    return text or "topic"


def utc_now() -> datetime:
    return datetime.now(UTC)


class SenderKind(StrEnum):
    HUMAN = "human"
    AGENT = "agent"
    SYSTEM = "system"


class MafiaGameStatus(StrEnum):
    LOBBY = "lobby"
    ACTIVE = "active"
    GAME_OVER = "game_over"


class MafiaPhase(StrEnum):
    LOBBY = "lobby"
    DAY_DISCUSSION = "day_discussion"
    DAY_VOTE = "day_vote"
    DAY_REVEAL = "day_reveal"
    NIGHT_ACTION = "night_action"
    NIGHT_REVEAL = "night_reveal"


class MafiaRole(StrEnum):
    TOWN = "town"
    MAFIA = "mafia"
    SPECTATOR = "spectator"


class MafiaFaction(StrEnum):
    TOWN = "town"
    MAFIA = "mafia"
    NONE = "none"


class MafiaTargetChoice(BaseModel):
    voter_participant_id: str
    target_participant_id: str | None = None


class MafiaRosterEntry(BaseModel):
    participant_id: str
    display_name: str
    is_human: bool = False
    alive: bool = True
    seat_index: int
    faction: MafiaFaction | None = None


class MafiaRevealRecord(BaseModel):
    phase: MafiaPhase
    participant_id: str | None = None
    display_name: str | None = None
    faction: MafiaFaction | None = None
    eliminated: bool = False
    reason: str = ""
    created_at: datetime = Field(default_factory=utc_now)


class MafiaPrivateState(BaseModel):
    participant_id: str
    role: MafiaRole = MafiaRole.SPECTATOR
    faction: MafiaFaction = MafiaFaction.NONE
    alive: bool = False
    can_chat: bool = False
    can_vote: bool = False
    can_act: bool = False
    legal_targets: list[str] = Field(default_factory=list)
    selected_target_participant_id: str | None = None
    teammates: list[str] = Field(default_factory=list)
    spectator: bool = True


class MafiaPublicState(BaseModel):
    game_status: MafiaGameStatus = MafiaGameStatus.LOBBY
    phase: MafiaPhase = MafiaPhase.LOBBY
    phase_started_at: datetime | None = None
    phase_ends_at: datetime | None = None
    total_players: int = 0
    round_no: int = 0
    roster: list[MafiaRosterEntry] = Field(default_factory=list)
    revealed_eliminations: list[MafiaRevealRecord] = Field(default_factory=list)
    winner: MafiaFaction | None = None
    winning_participant_ids: list[str] = Field(default_factory=list)


class MafiaPlayerRecord(BaseModel):
    participant_id: str
    display_name: str
    is_human: bool
    seat_index: int
    alive: bool = True
    role: MafiaRole = MafiaRole.TOWN
    faction: MafiaFaction = MafiaFaction.TOWN
    connected: bool = False


class MafiaGameSnapshot(BaseModel):
    game_status: MafiaGameStatus = MafiaGameStatus.LOBBY
    phase: MafiaPhase = MafiaPhase.LOBBY
    phase_started_at: datetime | None = None
    phase_ends_at: datetime | None = None
    total_players: int = 0
    round_no: int = 0
    players: list[MafiaPlayerRecord] = Field(default_factory=list)
    spectators: list[str] = Field(default_factory=list)
    revealed_eliminations: list[MafiaRevealRecord] = Field(default_factory=list)
    winner: MafiaFaction | None = None
    winning_participant_ids: list[str] = Field(default_factory=list)
    day_votes: dict[str, str] = Field(default_factory=dict)
    night_votes: dict[str, str] = Field(default_factory=dict)
    ready_humans: list[str] = Field(default_factory=list)

    def public_state(self) -> MafiaPublicState:
        return MafiaPublicState(
            game_status=self.game_status,
            phase=self.phase,
            phase_started_at=self.phase_started_at,
            phase_ends_at=self.phase_ends_at,
            total_players=self.total_players,
            round_no=self.round_no,
            roster=[
                MafiaRosterEntry(
                    participant_id=player.participant_id,
                    display_name=player.display_name,
                    is_human=player.is_human,
                    alive=player.alive,
                    seat_index=player.seat_index,
                    faction=player.faction if self.game_status == MafiaGameStatus.GAME_OVER else None,
                )
                for player in sorted(self.players, key=lambda item: item.seat_index)
            ],
            revealed_eliminations=list(self.revealed_eliminations),
            winner=self.winner,
            winning_participant_ids=list(self.winning_participant_ids),
        )

    def private_state_for(self, participant_id: str) -> MafiaPrivateState:
        player = next((item for item in self.players if item.participant_id == participant_id), None)
        if player is None:
            return MafiaPrivateState(participant_id=participant_id)
        teammates = [
            item.participant_id
            for item in self.players
            if item.participant_id != participant_id and item.alive and item.faction == MafiaFaction.MAFIA
        ]
        can_chat = player.alive and self.phase == MafiaPhase.DAY_DISCUSSION and self.game_status == MafiaGameStatus.ACTIVE
        can_vote = player.alive and self.phase == MafiaPhase.DAY_VOTE and self.game_status == MafiaGameStatus.ACTIVE
        can_act = (
            player.alive
            and player.faction == MafiaFaction.MAFIA
            and self.phase == MafiaPhase.NIGHT_ACTION
            and self.game_status == MafiaGameStatus.ACTIVE
        )
        legal_targets = []
        if can_vote:
            legal_targets = [item.participant_id for item in self.players if item.alive and item.participant_id != participant_id]
        elif can_act:
            legal_targets = [
                item.participant_id
                for item in self.players
                if item.alive and item.participant_id != participant_id and item.faction != MafiaFaction.MAFIA
            ]
        selected_target_participant_id = None
        if can_vote:
            selected_target_participant_id = self.day_votes.get(participant_id)
        elif can_act:
            selected_target_participant_id = self.night_votes.get(participant_id)
        return MafiaPrivateState(
            participant_id=participant_id,
            role=player.role,
            faction=player.faction,
            alive=player.alive,
            can_chat=can_chat,
            can_vote=can_vote,
            can_act=can_act,
            legal_targets=legal_targets,
            selected_target_participant_id=selected_target_participant_id,
            teammates=teammates if player.faction == MafiaFaction.MAFIA else [],
            spectator=False,
        )


class MafiaVoteInputSnapshot(BaseModel):
    scenario: str
    phase: MafiaPhase
    seconds_remaining: float = 0.0
    roster: list[MafiaRosterEntry] = Field(default_factory=list)
    recent_messages: list[ConversationMessage] = Field(default_factory=list)
    private_state: MafiaPrivateState
    legal_targets: list[str] = Field(default_factory=list)
    revealed_eliminations: list[MafiaRevealRecord] = Field(default_factory=list)


class MafiaVoteReply(BaseModel):
    target_participant_id: str | None = None
    reason: str = ""


class TopicWeight(BaseModel):
    topic_id: str
    weight: float

    @model_validator(mode="before")
    @classmethod
    def _coerce_weight(cls, value: Any) -> Any:
        if isinstance(value, str):
            return {"topic_id": f"topic-{_slugify_topic(value)}", "weight": 1.0}
        if isinstance(value, dict):
            data = dict(value)
            topic_id = data.get("topic_id") or data.get("label")
            if topic_id:
                data["topic_id"] = (
                    topic_id
                    if str(topic_id).startswith("topic-")
                    else f"topic-{_slugify_topic(str(topic_id))}"
                )
            data.setdefault("weight", 1.0)
            return data
        return value


class TopicSummary(BaseModel):
    topic_id: str = ""
    label: str
    keywords: list[str] = Field(default_factory=list)
    weight: float = 0.5
    confidence: float = 0.5

    @model_validator(mode="before")
    @classmethod
    def _coerce_topic(cls, value: Any) -> Any:
        if isinstance(value, str):
            return {
                "topic_id": f"topic-{_slugify_topic(value)}",
                "label": value,
                "keywords": [value],
                "weight": 0.5,
                "confidence": 0.5,
            }
        if isinstance(value, dict):
            data = dict(value)
            label = str(data.get("label") or data.get("name") or data.get("topic") or "").strip()
            keywords = data.get("keywords") or []
            if isinstance(keywords, str):
                keywords = [item.strip() for item in keywords.split(",") if item.strip()]
            if not label:
                if keywords:
                    label = str(keywords[0])
                elif data.get("topic_id"):
                    label = str(data["topic_id"]).replace("topic-", "").replace("-", " ")
            data["label"] = label or "topic"
            data["keywords"] = list(keywords) or [data["label"]]
            topic_id = data.get("topic_id") or data["label"]
            data["topic_id"] = (
                topic_id
                if str(topic_id).startswith("topic-")
                else f"topic-{_slugify_topic(str(topic_id))}"
            )
            data.setdefault("weight", 0.5)
            data.setdefault("confidence", 0.5)
            return data
        return value


class TopicShift(BaseModel):
    previous_topic_id: str | None = None
    current_topic_id: str | None = None
    shifted: bool = False


class ConversationMessage(BaseModel):
    message_id: str
    client_message_id: str
    sender_id: str
    sender_kind: SenderKind
    display_name: str
    text: str
    created_at: datetime
    sequence_no: int
    mentions: list[str] = Field(default_factory=list)
    reply_hint: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class MessagePayload(BaseModel):
    client_message_id: str
    sender_id: str
    sender_kind: SenderKind
    display_name: str
    text: str
    mentions: list[str] = Field(default_factory=list)
    reply_hint: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class CandidateRecord(BaseModel):
    candidate_id: str
    agent_id: str
    text: str
    created_at: datetime
    source_watermark: int = 0
    topic_snapshot_id: str | None = None
    dominant_topic_id_at_generation: str | None = None
    generation_keywords: list[str] = Field(default_factory=list)
    focus_message_ids: list[str] = Field(default_factory=list)
    topic_weights: list[TopicWeight] = Field(default_factory=list)
    buffer_version: int = 0
    score: float = 0.0
    score_breakdown: dict[str, float] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


class RoomMetricsSnapshot(BaseModel):
    watermark: int = 0
    active_participant_count: int = 0
    avg_message_rate: float = 0.0
    agent_message_rates: dict[str, float] = Field(default_factory=dict)
    recent_entropy: float = 0.0
    recent_shift_detected: bool = False
    recent_keyword_sketch: list[str] = Field(default_factory=list)
    time_since_last_any: float = 0.0


class ResponseSlotState(BaseModel):
    active: bool = False
    owner_id: str | None = None
    reason: str = "none"
    source_message_id: str | None = None


class OpenQuestionState(BaseModel):
    question_id: str
    source_message_id: str
    asker_id: str
    asker_display_name: str
    target_participant_id: str | None = None
    text_excerpt: str
    keyword_sketch: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=utc_now)
    resolved: bool = False
    resolved_by_message_id: str | None = None


class CommitmentState(BaseModel):
    source_message_id: str
    polarity: Literal["accepted", "rejected"]
    keyword_sketch: list[str] = Field(default_factory=list)
    canonical_text: str
    created_at: datetime = Field(default_factory=utc_now)


class RoomDiscourseStateSnapshot(BaseModel):
    strict_turn_active: bool = False
    slot_owner_id: str | None = None
    slot_reason: str = "none"
    slot_source_message_id: str | None = None
    open_questions: list[OpenQuestionState] = Field(default_factory=list)
    resolved_question_ids: list[str] = Field(default_factory=list)
    resolved_questions: list[OpenQuestionState] = Field(default_factory=list)
    accepted_commitments: list[CommitmentState] = Field(default_factory=list)
    rejected_commitments: list[CommitmentState] = Field(default_factory=list)
    last_strict_turn_owner_id: str | None = None


class AgentTopicSnapshot(BaseModel):
    snapshot_id: str
    agent_id: str
    watermark: int
    window_message_ids: list[str]
    topics: list[TopicSummary]
    message_topics: dict[str, list[TopicWeight]] = Field(default_factory=dict)
    dominant_topic_id: str | None = None
    shift: TopicShift = Field(default_factory=TopicShift)
    generated_at: datetime
    stale_after: datetime
    memory_summary: dict[str, float] = Field(default_factory=dict)


class AgentContextSnapshot(BaseModel):
    agent_id: str
    watermark: int
    current_time: datetime
    recent_messages: list[ConversationMessage]
    focus_messages: list[ConversationMessage]
    focus_message_ids: list[str] = Field(default_factory=list)
    room_is_idle: bool = False
    has_sent_message: bool = False
    own_message_count: int = 0
    recent_message_count: int = 0
    topic_snapshot_id: str | None = None
    topic_snapshot: AgentTopicSnapshot | None = None
    room_metrics: RoomMetricsSnapshot = Field(default_factory=RoomMetricsSnapshot)
    discourse_state: RoomDiscourseStateSnapshot = Field(default_factory=RoomDiscourseStateSnapshot)
    memory_summary: dict[str, float] = Field(default_factory=dict)
    active_participant_count: int = 0
    agent_message_rate: float = 0.0
    avg_message_rate: float = 0.0
    time_since_last_any: float = 0.0
    time_since_last_own: float = 0.0
    buffer_size: int = 0
    buffer_version: int = 0
    run_state: str = "idle"


class AnalyzerInputSnapshot(BaseModel):
    agent_id: str
    scenario: str
    recent_messages: list[ConversationMessage]
    previous_snapshot: AgentTopicSnapshot | None = None
    seed_topics: list[str] = Field(default_factory=list)


class GeneratorInputSnapshot(BaseModel):
    scenario: str
    agent_context: AgentContextSnapshot
    max_words: int
    style_prompt: str
    contribution_mode: Literal["concretize_constraints", "frame_requirements", "translate_into_interface", "generic_collaborator"] = "generic_collaborator"
    owns_response_slot: bool = False
    recent_open_questions: list[OpenQuestionState] = Field(default_factory=list)
    accepted_commitments: list[CommitmentState] = Field(default_factory=list)
    rejected_commitments: list[CommitmentState] = Field(default_factory=list)
    mafia_public_state: MafiaPublicState | None = None
    mafia_private_state: MafiaPrivateState | None = None


class SchedulerInputSnapshot(BaseModel):
    scenario: str
    agent_context: AgentContextSnapshot
    goals: list[str] = Field(default_factory=list)
    talk_mode: Literal["talkative", "listening"] = "listening"
    current_time_label: str
    has_buffered_candidate: bool = False
    candidate_preview_text: str | None = None
    candidate_similarity_score: float = 0.0
    similar_recent_message_id: str | None = None
    similar_recent_message_text: str | None = None
    similar_recent_message_age_seconds: float | None = None
    similar_recent_same_reply_target: bool = False
    similar_recent_same_turn_kind: bool = False
    inflight_similarity_score: float = 0.0
    similar_inflight_text: str | None = None
    similar_inflight_same_reply_target: bool = False
    similar_inflight_same_turn_kind: bool = False
    other_agents_typing_count: int = 0
    strict_turn_active: bool = False
    slot_owner_id: str | None = None
    slot_reason: str = "none"
    reply_target_message_id: str | None = None
    reply_target_speaker_id: str | None = None
    reply_target_display_name: str | None = None
    reply_target_reason: Literal["direct_mention", "reply_hint", "recent_question", "recent_turn", "none"] = "none"
    obligation_strength: Literal["none", "low", "medium", "high"] = "none"
    floor_state: Literal["open_floor", "addressed_response_slot", "brief_overlap_ok", "cooldown_after_self_turn"] = "open_floor"
    candidate_turn_kind: Literal["backchannel", "agreement", "answer", "challenge", "proposal", "repair", "summary", "pivot", "stance"] = "stance"
    candidate_matches_slot: bool = False
    candidate_answers_open_question_id: str | None = None
    recent_open_questions: list[OpenQuestionState] = Field(default_factory=list)
    recent_commitments: list[CommitmentState] = Field(default_factory=list)
    candidate_reopens_resolved_question: bool = False
    candidate_conflicts_with_commitment: bool = False
    candidate_supports_commitment: bool = False
    mafia_public_state: MafiaPublicState | None = None
    mafia_private_state: MafiaPrivateState | None = None


class SchedulerReply(BaseModel):
    decision: Literal["send", "wait"]
    reason: str = ""


class GeneratorReply(BaseModel):
    text: str


class AnalyzerReply(BaseModel):
    topics: list[TopicSummary]
    message_topics: dict[str, list[TopicWeight]] = Field(default_factory=dict)

    @field_validator("topics", mode="before")
    @classmethod
    def _coerce_topics(cls, value: Any) -> Any:
        if isinstance(value, dict):
            return list(value.values())
        if value is None:
            return []
        return value

    @field_validator("message_topics", mode="before")
    @classmethod
    def _coerce_message_topics(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return {}
        cleaned: dict[str, list[Any]] = {}
        for message_id, weights in value.items():
            if not isinstance(weights, list):
                continue
            cleaned[str(message_id)] = weights
        return cleaned


class DeliveryReservation(BaseModel):
    reservation_id: str
    agent_id: str
    candidate: CandidateRecord
    client_message_id: str
    created_at: datetime
    retry_count: int = 0
    state: Literal["reserved", "scheduled", "submitted", "acked", "committed", "aborted", "requeued", "expired"] = "reserved"
    message_id: str | None = None
    last_error: str | None = None


class TransportMessagePayload(BaseModel):
    reservation_id: str
    candidate_id: str
    client_message_id: str
    agent_id: str
    sender_id: str
    sender_kind: SenderKind
    display_name: str
    text: str
    mentions: list[str] = Field(default_factory=list)
    reply_hint: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class CommandEnvelope(BaseModel):
    command_id: str = Field(default_factory=lambda: str(uuid4()))
    subject: str
    schema_version: str = "1"
    timestamp: datetime = Field(default_factory=utc_now)
    correlation_id: str = Field(default_factory=lambda: str(uuid4()))
    causation_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class EventEnvelope(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid4()))
    subject: str
    schema_version: str = "1"
    timestamp: datetime = Field(default_factory=utc_now)
    correlation_id: str
    causation_id: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class LoggedEvent(BaseModel):
    seq: int
    event: EventEnvelope


def make_event(
    subject: str,
    *,
    command: CommandEnvelope | None = None,
    payload: BaseModel | dict[str, Any] | None = None,
) -> EventEnvelope:
    data: dict[str, Any]
    if payload is None:
        data = {}
    elif isinstance(payload, BaseModel):
        data = payload.model_dump(mode="json")
    else:
        data = _json_ready(dict(payload))
    return EventEnvelope(
        subject=subject,
        correlation_id=command.correlation_id if command else str(uuid4()),
        causation_id=command.command_id if command else None,
        payload=data,
    )


def _json_ready(value: Any) -> Any:
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json")
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [_json_ready(item) for item in value]
    return value
