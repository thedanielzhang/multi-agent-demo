from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from mafia.context import tokenize
from mafia.config import AgentConfig, AppConfig, ModeProfile
from mafia.messages import (
    AgentContextSnapshot,
    AgentTopicSnapshot,
    AnalyzerInputSnapshot,
    AnalyzerReply,
    CandidateRecord,
    DeliveryReservation,
    GeneratorInputSnapshot,
    RoomMetricsSnapshot,
    SchedulerInputSnapshot,
    TopicSummary,
    TopicWeight,
)


class PromptPolicy:
    def __init__(self, config: AppConfig) -> None:
        self._config = config

    def scheduler_prompt(self, agent: AgentConfig, snapshot: SchedulerInputSnapshot) -> str:
        history = "\n".join(
            f"[{message.created_at.astimezone(UTC).isoformat()}] {message.display_name}: {message.text}"
            for message in snapshot.agent_context.recent_messages
        ) or "(no messages yet)"
        persona = (
            f"talkativeness={agent.personality.talkativeness:.2f}, "
            f"confidence={agent.personality.confidence:.2f}, "
            f"reactivity={agent.personality.reactivity:.2f}, "
            f"topic_loyalty={agent.personality.topic_loyalty:.2f}"
        )
        if self._config.mode == ModeProfile.BASELINE_TIME_TO_TALK:
            return (
                "You are an agent in a Time-to-Talk style asynchronous group chat.\n"
                f"Scenario: {snapshot.scenario}\n"
                f"Goals: {', '.join(snapshot.goals) or '(none)'}\n"
                f"Persona: {persona}\n"
                f"Current time: {snapshot.current_time_label}\n"
                f"Current pacing mode: {snapshot.talk_mode}\n"
                "Decide strictly whether to `send` or `wait` right now.\n"
                "Use the pacing mode to decide if you should be more talkative or more listening.\n"
                "Conversation history with timestamps:\n"
                f"{history}\n"
                "Return JSON with `decision` and `reason`."
            )
        topic_summary = ", ".join(snapshot.agent_context.room_metrics.recent_keyword_sketch) or "(none)"
        candidate_preview = snapshot.candidate_preview_text or "(none)"
        duplicate_signal = (
            f"recent_received_similarity={snapshot.candidate_similarity_score:.2f}; "
            f"similar_recent_message={snapshot.similar_recent_message_text or '(none)'}; "
            f"similar_recent_age_seconds={snapshot.similar_recent_message_age_seconds if snapshot.similar_recent_message_age_seconds is not None else '(n/a)'}; "
            f"inflight_similarity={snapshot.inflight_similarity_score:.2f}; "
            f"similar_inflight_message={snapshot.similar_inflight_text or '(none)'}; "
            f"other_agents_typing={snapshot.other_agents_typing_count}"
        )
        return (
            "You are deciding whether to send a buffered candidate into an asynchronous group conversation.\n"
            f"Scenario: {snapshot.scenario}\n"
            f"Goals: {', '.join(snapshot.goals) or '(none)'}\n"
            f"Persona: {persona}\n"
            f"Current time: {snapshot.current_time_label}\n"
            f"Current pacing mode: {snapshot.talk_mode}\n"
            f"Room idle: {snapshot.agent_context.room_is_idle}\n"
            f"You have spoken before: {snapshot.agent_context.has_sent_message}\n"
            f"Seconds since any message: {snapshot.agent_context.time_since_last_any:.2f}\n"
            f"Seconds since your last message: {snapshot.agent_context.time_since_last_own:.2f}\n"
            f"Recent room keywords: {topic_summary}\n"
            f"Has buffered candidate: {snapshot.has_buffered_candidate}\n"
            f"Buffered candidate preview: {candidate_preview}\n"
            f"Duplicate suppression signal: {duplicate_signal}\n"
            "If the room is quiet or nobody has started yet, sending a short opener is appropriate when your candidate is strong.\n"
            "If the buffered candidate feels too similar to something that was just said or is already being typed by another agent, prefer `wait`.\n"
            "Conversation focus window:\n"
            f"{history}\n"
            "Return JSON with `decision` and `reason`."
        )

    def generator_prompt(self, agent: AgentConfig, snapshot: GeneratorInputSnapshot) -> str:
        history = "\n".join(
            f"{message.display_name}: {message.text}" for message in snapshot.agent_context.focus_messages
        ) or "(no recent messages)"
        persona = (
            f"talkativeness={agent.personality.talkativeness:.2f}, "
            f"confidence={agent.personality.confidence:.2f}, "
            f"reactivity={agent.personality.reactivity:.2f}, "
            f"topic_loyalty={agent.personality.topic_loyalty:.2f}"
        )
        if self._config.mode == ModeProfile.BASELINE_TIME_TO_TALK:
            return (
                "Write one natural chat message that fits the current conversation.\n"
                f"You are {agent.display_name}.\n"
                f"Scenario: {snapshot.scenario}\n"
                f"Goals: {', '.join(agent.goals) or '(none)'}\n"
                f"Persona: {persona}\n"
                f"Style guidance: {snapshot.style_prompt}\n"
                f"Maximum words: {snapshot.max_words}\n"
                "Conversation history:\n"
                f"{history}\n"
                "Return JSON with `text` only."
            )
        topic_summary = ", ".join(snapshot.agent_context.room_metrics.recent_keyword_sketch) or "(none)"
        return (
            "Write one candidate message for a buffered asynchronous conversation.\n"
            f"You are {agent.display_name}.\n"
            f"Scenario: {snapshot.scenario}\n"
            f"Goals: {', '.join(agent.goals) or '(none)'}\n"
            f"Persona: {persona}\n"
            f"Style guidance: {snapshot.style_prompt}\n"
            f"Maximum words: {snapshot.max_words}\n"
            f"Current room keywords: {topic_summary}\n"
            "Focused context window:\n"
            f"{history}\n"
            "Return JSON with `text` only."
        )

    def analyzer_prompt(self, snapshot: AnalyzerInputSnapshot) -> str:
        history = "\n".join(f"{message.display_name}: {message.text}" for message in snapshot.recent_messages) or "(none)"
        previous = []
        if snapshot.previous_snapshot:
            previous = [
                f"{topic.topic_id}:{topic.label}:{','.join(topic.keywords)}"
                for topic in snapshot.previous_snapshot.topics
            ]
        return (
            "Analyze the conversation window and extract 1-3 concise topics as JSON.\n"
            f"Scenario: {snapshot.scenario}\n"
            f"Seed topics: {', '.join(snapshot.seed_topics) or '(none)'}\n"
            f"Prior stable topics: {', '.join(previous) or '(none)'}\n"
            "Conversation window:\n"
            f"{history}\n"
            "Return JSON with `topics` and `message_topics`."
        )


class PolicySet:
    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self.prompts = PromptPolicy(config)

    def scheduler_input(
        self,
        agent: AgentConfig,
        context: AgentContextSnapshot,
        *,
        buffer_candidates: list[CandidateRecord] | None = None,
        active_reservations: list[DeliveryReservation] | None = None,
    ) -> SchedulerInputSnapshot:
        active = max(1, context.active_participant_count)
        talk_mode: Literal["talkative", "listening"] = "talkative"
        if context.agent_message_rate > (1.0 / active):
            talk_mode = "listening"
        candidate_preview_text: str | None = None
        candidate_similarity_score = 0.0
        similar_recent_message_text: str | None = None
        similar_recent_message_age_seconds: float | None = None
        inflight_similarity_score = 0.0
        similar_inflight_text: str | None = None
        reservations = list(active_reservations or [])
        candidate = None
        if buffer_candidates:
            selected = self.select_best_candidate(agent, context, buffer_candidates, context.current_time)
            if selected is not None:
                candidate, _breakdown = selected
                candidate_preview_text = candidate.text
                candidate_similarity_score, similar_recent_message_text, similar_recent_message_age_seconds = (
                    self._best_recent_message_similarity(context, candidate)
                )
                inflight_similarity_score, similar_inflight_text = self._best_inflight_similarity(
                    agent,
                    reservations,
                    candidate,
                )
        return SchedulerInputSnapshot(
            scenario=self._config.chat.scenario,
            agent_context=context,
            goals=agent.goals,
            talk_mode=talk_mode,
            current_time_label=context.current_time.astimezone(UTC).isoformat(),
            has_buffered_candidate=bool(candidate_preview_text),
            candidate_preview_text=candidate_preview_text,
            candidate_similarity_score=candidate_similarity_score,
            similar_recent_message_text=similar_recent_message_text,
            similar_recent_message_age_seconds=similar_recent_message_age_seconds,
            inflight_similarity_score=inflight_similarity_score,
            similar_inflight_text=similar_inflight_text,
            other_agents_typing_count=sum(1 for reservation in reservations if reservation.agent_id != agent.id),
        )

    def generator_input(self, agent: AgentConfig, context: AgentContextSnapshot) -> GeneratorInputSnapshot:
        return GeneratorInputSnapshot(
            scenario=self._config.chat.scenario,
            agent_context=context,
            max_words=agent.max_words,
            style_prompt=agent.style_prompt,
        )

    def analyzer_input(
        self,
        agent: AgentConfig,
        context: AgentContextSnapshot,
        previous_snapshot: AgentTopicSnapshot | None,
    ) -> AnalyzerInputSnapshot:
        seed_topics = [token for token in self._config.chat.scenario.lower().split() if len(token) > 4][:3]
        return AnalyzerInputSnapshot(
            agent_id=agent.id,
            scenario=self._config.chat.scenario,
            recent_messages=list(context.focus_messages),
            previous_snapshot=previous_snapshot,
            seed_topics=seed_topics,
        )

    def typing_delay(self, text: str) -> float:
        words = max(1, len(text.split()))
        return words / max(0.1, self._config.chat.typing_words_per_second)

    def should_generate(self, context: AgentContextSnapshot, buffer_limit: int) -> bool:
        if self._config.mode == ModeProfile.BASELINE_TIME_TO_TALK:
            return False
        return context.buffer_size < buffer_limit and context.run_state == "running"

    def candidate_is_stale(self, agent: AgentConfig, candidate: CandidateRecord, now: datetime) -> bool:
        age = max(0.0, (now - candidate.created_at).total_seconds())
        return age >= max(0.1, agent.generation.staleness_window_seconds)

    def score_candidate(
        self,
        agent: AgentConfig,
        context: AgentContextSnapshot,
        candidate: CandidateRecord,
        now: datetime,
    ) -> tuple[float, dict[str, float]]:
        staleness_window = max(0.1, agent.generation.staleness_window_seconds)
        age = max(0.0, (now - candidate.created_at).total_seconds())
        freshness = max(0.0, 1.0 - (age / staleness_window))
        current_keywords = set(context.room_metrics.recent_keyword_sketch)
        if context.topic_snapshot:
            dominant = next(
                (topic for topic in context.topic_snapshot.topics if topic.topic_id == context.topic_snapshot.dominant_topic_id),
                None,
            )
            if dominant:
                current_keywords.update(keyword.lower() for keyword in dominant.keywords)
        generation_keywords = {keyword.lower() for keyword in candidate.generation_keywords}
        topic_fit = len(current_keywords & generation_keywords) / max(1, len(current_keywords | generation_keywords))
        room_tokens = {
            token.lower()
            for message in context.focus_messages
            for token in message.text.split()
        }
        candidate_tokens = set(candidate.text.lower().split())
        lexical_fit = len(room_tokens & candidate_tokens) / max(1, len(room_tokens | candidate_tokens))
        shift_penalty = 0.0
        if current_keywords and generation_keywords and not (current_keywords & generation_keywords):
            shift_penalty = self._config.context_for(agent).shift_penalty * (
                1.0 + agent.personality.topic_loyalty - (agent.personality.confidence * 0.5)
            )
        score = max(0.0, (freshness * 0.45) + (topic_fit * 0.4) + (lexical_fit * 0.15) - shift_penalty)
        breakdown = {
            "freshness": freshness,
            "topic_fit": topic_fit,
            "lexical_fit": lexical_fit,
            "shift_penalty": shift_penalty,
            "composite": score,
        }
        return score, breakdown

    def select_best_candidate(
        self,
        agent: AgentConfig,
        context: AgentContextSnapshot,
        candidates: list[CandidateRecord],
        now: datetime,
    ) -> tuple[CandidateRecord, dict[str, float]] | None:
        if not candidates:
            return None
        scored: list[tuple[CandidateRecord, dict[str, float]]] = []
        for candidate in candidates:
            score, breakdown = self.score_candidate(agent, context, candidate, now)
            candidate.score = score
            candidate.score_breakdown = breakdown
            scored.append((candidate, breakdown))
        return max(scored, key=lambda item: item[0].score)

    def reconcile_topics(
        self,
        agent: AgentConfig,
        previous_snapshot: AgentTopicSnapshot | None,
        reply: AnalyzerReply,
    ) -> tuple[list[TopicSummary], dict[str, list[TopicWeight]], dict[str, float]]:
        previous_topics = previous_snapshot.topics if previous_snapshot else []
        stable_topics: list[TopicSummary] = []
        label_to_stable_id: dict[str, str] = {}
        previous_by_id = {topic.topic_id: topic for topic in previous_topics}

        def resolve_topic_id(topic: TopicSummary) -> str:
            topic_keywords = {keyword.lower() for keyword in topic.keywords} | {topic.label.lower()}
            best_topic_id: str | None = None
            best_overlap = 0.0
            for previous in previous_topics:
                previous_keywords = {keyword.lower() for keyword in previous.keywords} | {previous.label.lower()}
                overlap = len(topic_keywords & previous_keywords) / max(1, len(topic_keywords | previous_keywords))
                if overlap > best_overlap:
                    best_overlap = overlap
                    best_topic_id = previous.topic_id
            if best_topic_id and best_overlap >= 0.5:
                return best_topic_id
            return f"topic-{topic.label.lower().replace(' ', '-')}"

        for topic in reply.topics[: self._config.topic.max_topics]:
            stable_id = resolve_topic_id(topic)
            label_to_stable_id[topic.topic_id] = stable_id
            label_to_stable_id[topic.label.lower()] = stable_id
            stable_topics.append(
                TopicSummary(
                    topic_id=stable_id,
                    label=topic.label,
                    keywords=list(dict.fromkeys([keyword.lower() for keyword in topic.keywords])),
                    weight=topic.weight,
                    confidence=topic.confidence,
                )
            )

        stable_message_topics: dict[str, list[TopicWeight]] = {}
        for message_id, weights in reply.message_topics.items():
            stable_weights: list[TopicWeight] = []
            for weight in weights:
                stable_id = label_to_stable_id.get(weight.topic_id, weight.topic_id)
                stable_weights.append(TopicWeight(topic_id=stable_id, weight=weight.weight))
            stable_message_topics[message_id] = stable_weights

        context_config = self._config.context_for(agent)
        memory_summary = dict(previous_snapshot.memory_summary) if previous_snapshot else {}
        for topic in stable_topics:
            previous_value = memory_summary.get(topic.topic_id, 0.0)
            memory_summary[topic.topic_id] = (previous_value * context_config.memory_decay) + topic.weight
        for topic_id in list(memory_summary):
            if topic_id not in {topic.topic_id for topic in stable_topics}:
                memory_summary[topic_id] *= context_config.memory_decay
                if memory_summary[topic_id] <= 0.05:
                    memory_summary.pop(topic_id)

        return stable_topics, stable_message_topics, memory_summary

    def consecutive_failure_is_fatal(self, failures: int) -> bool:
        return failures >= 3

    def _best_recent_message_similarity(
        self,
        context: AgentContextSnapshot,
        candidate: CandidateRecord,
    ) -> tuple[float, str | None, float | None]:
        best_score = 0.0
        best_text: str | None = None
        best_age: float | None = None
        for message in context.recent_messages[-6:]:
            score = _text_similarity(candidate.text, message.text)
            if score <= best_score:
                continue
            best_score = score
            best_text = message.text
            best_age = max(0.0, (context.current_time - message.created_at).total_seconds())
        return best_score, best_text, best_age

    def _best_inflight_similarity(
        self,
        agent: AgentConfig,
        active_reservations: list[DeliveryReservation],
        candidate: CandidateRecord,
    ) -> tuple[float, str | None]:
        best_score = 0.0
        best_text: str | None = None
        for reservation in active_reservations:
            if reservation.agent_id == agent.id:
                continue
            score = _text_similarity(candidate.text, reservation.candidate.text)
            if score <= best_score:
                continue
            best_score = score
            best_text = reservation.candidate.text
        return best_score, best_text


def _text_similarity(left: str, right: str) -> float:
    normalized_left = " ".join(left.lower().split())
    normalized_right = " ".join(right.lower().split())
    if not normalized_left or not normalized_right:
        return 0.0
    if normalized_left == normalized_right:
        return 1.0
    left_tokens = set(tokenize(normalized_left))
    right_tokens = set(tokenize(normalized_right))
    if not left_tokens or not right_tokens:
        return 0.0
    return len(left_tokens & right_tokens) / max(1, len(left_tokens | right_tokens))
