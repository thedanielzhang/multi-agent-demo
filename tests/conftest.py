from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from mafia.config import (  # noqa: E402
    AgentConfig,
    AppConfig,
    ChatConfig,
    ContextConfig,
    GenerationConfig,
    ModeProfile,
    PersonalityConfig,
    RuntimeConfig,
    SchedulerConfig,
    TopicConfig,
)


@pytest.fixture
def baseline_config() -> AppConfig:
    return AppConfig(
        mode=ModeProfile.BASELINE_TIME_TO_TALK,
        runtime=RuntimeConfig(provider="scripted"),
        chat=ChatConfig(
            scenario="You are coworkers deciding where to go for lunch.",
            max_duration_seconds=2.0,
            max_messages=12,
            typing_words_per_second=20.0,
        ),
        generation=GenerationConfig(tick_rate_seconds=0.2, buffer_size=3, staleness_window_seconds=10.0),
        topic=TopicConfig(enabled=False, tick_rate_seconds=0.3, stale_after_seconds=10.0, max_topics=3),
        context_defaults=ContextConfig(
            recent_window_messages=10,
            focus_window_messages=5,
            memory_decay=0.8,
            recency_weight=1.0,
            topic_weight=1.0,
            mention_weight=1.2,
            own_message_weight=1.1,
            shift_penalty=0.15,
        ),
        agents=[
            AgentConfig(
                id="alex",
                display_name="Alex",
                goals=["keep the conversation moving"],
                style_prompt="casual and short",
                max_words=8,
                personality=PersonalityConfig(talkativeness=0.9, confidence=0.7, reactivity=0.8, topic_loyalty=0.4),
                scheduler=SchedulerConfig(tick_rate_seconds=0.2),
                generation=GenerationConfig(tick_rate_seconds=0.2, buffer_size=3, staleness_window_seconds=10.0),
            ),
            AgentConfig(
                id="jordan",
                display_name="Jordan",
                goals=["be thoughtful"],
                style_prompt="measured",
                max_words=10,
                personality=PersonalityConfig(talkativeness=0.7, confidence=0.8, reactivity=0.5, topic_loyalty=0.6),
                scheduler=SchedulerConfig(tick_rate_seconds=0.25),
                generation=GenerationConfig(tick_rate_seconds=0.2, buffer_size=3, staleness_window_seconds=10.0),
            ),
            AgentConfig(
                id="casey",
                display_name="Casey",
                goals=["add options"],
                style_prompt="friendly",
                max_words=10,
                personality=PersonalityConfig(talkativeness=0.6, confidence=0.6, reactivity=0.6, topic_loyalty=0.5),
                scheduler=SchedulerConfig(tick_rate_seconds=0.3),
                generation=GenerationConfig(tick_rate_seconds=0.2, buffer_size=3, staleness_window_seconds=10.0),
            ),
        ],
    )


@pytest.fixture
def improved_config(baseline_config: AppConfig) -> AppConfig:
    data = baseline_config.model_dump()
    data["mode"] = ModeProfile.IMPROVED_BUFFERED_ASYNC
    data["topic"]["enabled"] = True
    return AppConfig.model_validate(data)
