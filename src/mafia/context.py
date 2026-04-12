from __future__ import annotations

import math
import re
from collections import Counter

from mafia.config import AgentConfig, AppConfig, ContextConfig, ModeProfile
from mafia.messages import AgentContextSnapshot, AgentTopicSnapshot, ConversationMessage, RoomMetricsSnapshot

_TOKEN_RE = re.compile(r"[a-z0-9']+")


def tokenize(text: str) -> list[str]:
    return [token for token in _TOKEN_RE.findall(text.lower()) if len(token) > 2]


class ContextAssembler:
    """Builds agent-local context windows from committed conversation."""

    def __init__(self, config: AppConfig) -> None:
        self._config = config

    def build(
        self,
        agent: AgentConfig,
        *,
        watermark: int,
        current_time,
        recent_messages: list[ConversationMessage],
        room_metrics: RoomMetricsSnapshot,
        topic_snapshot: AgentTopicSnapshot | None,
        buffer_size: int,
        buffer_version: int,
        run_state: str,
    ) -> AgentContextSnapshot:
        if self._config.mode == ModeProfile.BASELINE_TIME_TO_TALK:
            focus_messages = list(recent_messages)
        else:
            focus_messages = self._smart_window(
                agent,
                recent_messages=recent_messages,
                topic_snapshot=topic_snapshot,
            )
        agent_messages = [message for message in recent_messages if message.sender_id == agent.id]
        room_is_idle = not recent_messages
        time_since_last_own = 9999.0 if room_is_idle else 0.0
        if agent_messages:
            time_since_last_own = max(
                0.0,
                (current_time - agent_messages[-1].created_at).total_seconds(),
            )
        elif recent_messages:
            time_since_last_own = room_metrics.time_since_last_any + 1.0
        memory_summary = topic_snapshot.memory_summary if topic_snapshot else {}
        return AgentContextSnapshot(
            agent_id=agent.id,
            watermark=watermark,
            current_time=current_time,
            recent_messages=recent_messages,
            focus_messages=focus_messages,
            focus_message_ids=[message.message_id for message in focus_messages],
            room_is_idle=room_is_idle,
            has_sent_message=bool(agent_messages),
            own_message_count=len(agent_messages),
            recent_message_count=len(recent_messages),
            topic_snapshot_id=topic_snapshot.snapshot_id if topic_snapshot else None,
            topic_snapshot=topic_snapshot.model_copy(deep=True) if topic_snapshot else None,
            room_metrics=room_metrics,
            memory_summary=memory_summary,
            active_participant_count=room_metrics.active_participant_count,
            agent_message_rate=room_metrics.agent_message_rates.get(agent.id, 0.0),
            avg_message_rate=room_metrics.avg_message_rate,
            time_since_last_any=room_metrics.time_since_last_any,
            time_since_last_own=time_since_last_own,
            buffer_size=buffer_size,
            buffer_version=buffer_version,
            run_state=run_state,
        )

    def _smart_window(
        self,
        agent: AgentConfig,
        *,
        recent_messages: list[ConversationMessage],
        topic_snapshot: AgentTopicSnapshot | None,
    ) -> list[ConversationMessage]:
        if not recent_messages:
            return []
        settings = self._config.context_for(agent)
        scored: list[tuple[float, ConversationMessage]] = []
        dominant_keywords = set()
        if topic_snapshot:
            for topic in topic_snapshot.topics:
                if topic.topic_id == topic_snapshot.dominant_topic_id:
                    dominant_keywords = {keyword.lower() for keyword in topic.keywords}
                    break
        latest_seq = recent_messages[-1].sequence_no
        for message in recent_messages[-settings.recent_window_messages :]:
            score = self._score_message(message, latest_seq, agent, settings, dominant_keywords)
            scored.append((score, message))
        top = sorted(scored, key=lambda item: item[0], reverse=True)[: settings.focus_window_messages]
        return sorted((message for _, message in top), key=lambda message: message.sequence_no)

    def _score_message(
        self,
        message: ConversationMessage,
        latest_seq: int,
        agent: AgentConfig,
        settings: ContextConfig,
        dominant_keywords: set[str],
    ) -> float:
        age = max(0, latest_seq - message.sequence_no)
        recency_score = settings.recency_weight * (1.0 / (1.0 + age))
        mention_score = settings.mention_weight if agent.id in message.mentions else 0.0
        own_score = settings.own_message_weight if message.sender_id == agent.id else 0.0
        tokens = set(tokenize(message.text))
        topic_score = settings.topic_weight * (len(tokens & dominant_keywords) / max(1, len(dominant_keywords)))
        personality = agent.personality
        personality_bias = 1.0 + (personality.reactivity * 0.2) + (personality.topic_loyalty * topic_score * 0.25)
        return personality_bias * (recency_score + mention_score + own_score + topic_score)

def keyword_sketch(messages: list[ConversationMessage], limit: int = 5) -> list[str]:
    counter: Counter[str] = Counter()
    for message in messages:
        counter.update(tokenize(message.text))
    return [token for token, _ in counter.most_common(limit)]


def entropy_from_keywords(messages: list[ConversationMessage]) -> float:
    counter: Counter[str] = Counter()
    for message in messages:
        counter.update(keyword_sketch([message], limit=3))
    total = sum(counter.values())
    if total == 0:
        return 0.0
    entropy = 0.0
    for count in counter.values():
        probability = count / total
        entropy -= probability * math.log(probability, 2)
    return min(1.0, entropy / 3.0)
