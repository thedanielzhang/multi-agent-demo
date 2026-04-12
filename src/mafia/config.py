from __future__ import annotations

from enum import StrEnum
from pydantic import BaseModel, Field


class ModeProfile(StrEnum):
    BASELINE_TIME_TO_TALK = "baseline.time_to_talk"
    IMPROVED_BUFFERED_ASYNC = "improved.buffered_async"


class RoomMode(StrEnum):
    REGULAR = "regular"
    MAFIA = "mafia"


class RuntimeConfig(BaseModel):
    provider: str = "claude"
    model: str = "claude-haiku-4-5-20251001"
    max_concurrency: int | None = Field(default=None, ge=1)


class TransportConfig(BaseModel):
    provider: str = "loopback"


class ChatConfig(BaseModel):
    scenario: str
    max_duration_seconds: float | None = None
    max_messages: int | None = None
    typing_words_per_second: float = 1.0


class GenerationConfig(BaseModel):
    tick_rate_seconds: float = 0.5
    buffer_size: int = 5
    staleness_window_seconds: float = 30.0


class SchedulerConfig(BaseModel):
    tick_rate_seconds: float = 1.0


class TopicConfig(BaseModel):
    enabled: bool = True
    tick_rate_seconds: float = 3.0
    stale_after_seconds: float = 10.0
    max_topics: int = 3


class MafiaConfig(BaseModel):
    total_players: int = Field(default=6, ge=5, le=13)
    day_discussion_seconds: float = 270.0
    day_vote_seconds: float = 90.0
    day_reveal_seconds: float = 30.0
    night_action_seconds: float = 90.0
    night_reveal_seconds: float = 30.0


class ContextConfig(BaseModel):
    recent_window_messages: int = 12
    focus_window_messages: int = 6
    memory_decay: float = 0.8
    recency_weight: float = 1.0
    topic_weight: float = 1.0
    mention_weight: float = 1.5
    own_message_weight: float = 1.25
    shift_penalty: float = 0.2
    debug_verbose: bool = False


class PersonalityConfig(BaseModel):
    talkativeness: float = 0.5
    confidence: float = 0.5
    reactivity: float = 0.5
    topic_loyalty: float = 0.5


class AgentConfig(BaseModel):
    id: str
    display_name: str
    goals: list[str] = Field(default_factory=list)
    style_prompt: str = "Speak naturally."
    max_words: int = 20
    personality: PersonalityConfig = Field(default_factory=PersonalityConfig)
    scheduler: SchedulerConfig = Field(default_factory=SchedulerConfig)
    generation: GenerationConfig = Field(default_factory=GenerationConfig)
    context: ContextConfig | None = None


class PolicyProfile(BaseModel):
    scheduling_policy: str
    generation_policy: str
    context_policy: str
    prompt_policy: str


class AppConfig(BaseModel):
    room_mode: RoomMode = RoomMode.REGULAR
    mode: ModeProfile = ModeProfile.IMPROVED_BUFFERED_ASYNC
    runtime: RuntimeConfig = Field(default_factory=RuntimeConfig)
    transport: TransportConfig = Field(default_factory=TransportConfig)
    chat: ChatConfig
    generation: GenerationConfig = Field(default_factory=GenerationConfig)
    topic: TopicConfig = Field(default_factory=TopicConfig)
    mafia: MafiaConfig = Field(default_factory=MafiaConfig)
    context_defaults: ContextConfig = Field(default_factory=ContextConfig)
    agents: list[AgentConfig]

    @property
    def policy_profile(self) -> PolicyProfile:
        if self.mode == ModeProfile.BASELINE_TIME_TO_TALK:
            return PolicyProfile(
                scheduling_policy="serial.time_to_talk",
                generation_policy="on_demand",
                context_policy="full_history",
                prompt_policy="paper_baseline",
            )
        return PolicyProfile(
            scheduling_policy="buffered.async",
            generation_policy="continuous_buffer",
            context_policy="smart_window",
            prompt_policy="topic_aware",
        )

    def context_for(self, agent: AgentConfig) -> ContextConfig:
        if agent.context is None:
            return self.context_defaults.model_copy(deep=True)
        merged = self.context_defaults.model_dump()
        merged.update(agent.context.model_dump(exclude_unset=True))
        return ContextConfig.model_validate(merged)
