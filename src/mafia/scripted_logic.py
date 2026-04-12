from __future__ import annotations

from collections import Counter
from typing import Any

from mafia.context import tokenize
from mafia.messages import (
    AnalyzerInputSnapshot,
    AnalyzerReply,
    GeneratorInputSnapshot,
    GeneratorReply,
    MafiaVoteInputSnapshot,
    MafiaVoteReply,
    SchedulerInputSnapshot,
    SchedulerReply,
    TopicSummary,
    TopicWeight,
)


class ScriptedAgentLogic:
    """Deterministic agent behavior owned by the service, not the runtime wrapper."""

    def scheduler_reply(
        self,
        snapshot: SchedulerInputSnapshot,
        metadata: dict[str, Any],
    ) -> SchedulerReply:
        talkativeness = float(metadata.get("talkativeness", 0.5))
        confidence = float(metadata.get("confidence", 0.5))
        reactivity = float(metadata.get("reactivity", 0.5))
        context = snapshot.agent_context
        if context.run_state != "running":
            return SchedulerReply(decision="wait", reason="run-not-active")
        if context.room_is_idle and snapshot.has_buffered_candidate:
            opening_pressure = talkativeness + (confidence * 0.3)
            return SchedulerReply(
                decision="send" if opening_pressure >= 0.35 else "wait",
                reason="room-opening" if opening_pressure >= 0.35 else "room-idle-wait",
            )
        if context.has_sent_message and context.time_since_last_own < max(0.12, 0.72 - (talkativeness * 0.45)):
            return SchedulerReply(decision="wait", reason="recent-own-message")
        if context.time_since_last_any < max(0.04, 0.24 - (reactivity * 0.1)):
            return SchedulerReply(decision="wait", reason="conversation-active")
        if (
            snapshot.has_buffered_candidate
            and snapshot.candidate_similarity_score >= 0.96
            and (snapshot.similar_recent_message_age_seconds or 999.0) <= 4.0
        ):
            return SchedulerReply(decision="wait", reason="duplicate-recent-message")
        if snapshot.has_buffered_candidate and snapshot.inflight_similarity_score >= 0.97:
            return SchedulerReply(decision="wait", reason="duplicate-inflight-message")

        baseline_pressure = talkativeness + (confidence * 0.25)
        buffered_pressure = talkativeness + (confidence * 0.2)
        if snapshot.talk_mode == "talkative":
            buffered_pressure += 0.1

        if snapshot.has_buffered_candidate and context.buffer_size > 0:
            return SchedulerReply(
                decision="send" if buffered_pressure >= 0.28 else "wait",
                reason="buffered-policy",
            )
        return SchedulerReply(
            decision="send" if baseline_pressure >= 0.35 else "wait",
            reason="baseline-policy",
        )

    def generator_reply(
        self,
        snapshot: GeneratorInputSnapshot,
        metadata: dict[str, Any],
    ) -> GeneratorReply:
        display_name = metadata.get("agent_id", "agent")
        style_prompt = snapshot.style_prompt.lower()
        recent = snapshot.agent_context.focus_messages
        if not recent:
            text = f"i'm {display_name}."
        else:
            last = recent[-1].text.strip(" .!?")
            keywords = tokenize(last)
            lead = keywords[0] if keywords else "that"
            variation_seed = len(display_name) + len(recent)
            if "casual" in style_prompt or "slang" in style_prompt:
                options = [
                    f"i'm down for {lead}",
                    f"{lead} could work for me",
                    f"honestly {lead} sounds solid",
                ]
                text = options[variation_seed % len(options)]
            else:
                options = [
                    f"I'd be up for {lead}.",
                    f"I think {lead} could work.",
                    f"{lead} seems like a good option.",
                ]
                text = options[variation_seed % len(options)]
        words = text.split()[: snapshot.max_words]
        return GeneratorReply(text=" ".join(words))

    def analyzer_reply(self, snapshot: AnalyzerInputSnapshot) -> AnalyzerReply:
        counter: Counter[str] = Counter()
        per_message: dict[str, list[TopicWeight]] = {}
        for message in snapshot.recent_messages:
            tokens = tokenize(message.text)
            counter.update(tokens)

        topics: list[TopicSummary] = []
        previous_by_label = {}
        if snapshot.previous_snapshot:
            previous_by_label = {topic.label.lower(): topic.topic_id for topic in snapshot.previous_snapshot.topics}

        common = [token for token, _ in counter.most_common(3)]
        for token in common:
            topic_id = previous_by_label.get(token, f"topic-{token}")
            topics.append(
                TopicSummary(
                    topic_id=topic_id,
                    label=token,
                    keywords=[token],
                    weight=counter[token] / max(1, sum(counter.values())),
                    confidence=0.8,
                )
            )

        for message in snapshot.recent_messages:
            weights = []
            tokens = set(tokenize(message.text))
            for topic in topics:
                if topic.label in tokens:
                    weights.append(TopicWeight(topic_id=topic.topic_id, weight=1.0))
            per_message[message.message_id] = weights or (
                [TopicWeight(topic_id=topics[0].topic_id, weight=0.5)] if topics else []
            )

        return AnalyzerReply(topics=topics, message_topics=per_message)

    def mafia_vote_reply(
        self,
        snapshot: MafiaVoteInputSnapshot,
        metadata: dict[str, Any],
    ) -> MafiaVoteReply:
        del metadata
        if not snapshot.legal_targets:
            return MafiaVoteReply(target_participant_id=None, reason="no-legal-targets")
        if snapshot.phase.value == "night_action":
            target = snapshot.legal_targets[0]
            return MafiaVoteReply(target_participant_id=target, reason="night-target")
        recent_text = " ".join(message.text for message in snapshot.recent_messages[-3:]).lower()
        for target in snapshot.legal_targets:
            if target.lower() in recent_text:
                return MafiaVoteReply(target_participant_id=target, reason="mentioned-target")
        return MafiaVoteReply(target_participant_id=snapshot.legal_targets[0], reason="first-legal-target")
