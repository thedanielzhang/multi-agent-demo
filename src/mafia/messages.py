from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, Field


def utc_now() -> datetime:
    return datetime.now(UTC)


class SenderKind(StrEnum):
    HUMAN = "human"
    AGENT = "agent"
    SYSTEM = "system"


class TopicWeight(BaseModel):
    topic_id: str
    weight: float


class TopicSummary(BaseModel):
    topic_id: str
    label: str
    keywords: list[str]
    weight: float
    confidence: float


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


class SchedulerInputSnapshot(BaseModel):
    scenario: str
    agent_context: AgentContextSnapshot
    goals: list[str] = Field(default_factory=list)
    talk_mode: Literal["talkative", "listening"] = "listening"
    current_time_label: str
    has_buffered_candidate: bool = False
    candidate_preview_text: str | None = None
    candidate_similarity_score: float = 0.0
    similar_recent_message_text: str | None = None
    similar_recent_message_age_seconds: float | None = None
    inflight_similarity_score: float = 0.0
    similar_inflight_text: str | None = None
    other_agents_typing_count: int = 0


class SchedulerReply(BaseModel):
    decision: Literal["send", "wait"]
    reason: str = ""


class GeneratorReply(BaseModel):
    text: str


class AnalyzerReply(BaseModel):
    topics: list[TopicSummary]
    message_topics: dict[str, list[TopicWeight]] = Field(default_factory=dict)


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
