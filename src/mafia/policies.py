from __future__ import annotations

import re
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
    CommitmentState,
    DeliveryReservation,
    GeneratorInputSnapshot,
    MafiaPhase,
    MafiaPrivateState,
    MafiaPublicState,
    MafiaVoteInputSnapshot,
    OpenQuestionState,
    ProposalState,
    RoomMetricsSnapshot,
    RoomDiscourseStateSnapshot,
    SchedulerInputSnapshot,
    TopicSummary,
    TopicWeight,
)

_QUESTION_STARTERS = {
    "who",
    "what",
    "when",
    "where",
    "why",
    "how",
    "which",
    "is",
    "are",
    "am",
    "do",
    "does",
    "did",
    "can",
    "could",
    "would",
    "should",
    "will",
    "was",
    "were",
    "anyone",
}
_BACKCHANNEL_TOKENS = {
    "yeah",
    "yep",
    "yup",
    "nah",
    "nope",
    "true",
    "same",
    "fair",
    "right",
    "ok",
    "okay",
    "exactly",
    "agreed",
    "sure",
}
_AGREEMENT_PHRASES = (
    "i agree",
    "agreed",
    "same here",
    "same",
    "good point",
    "fair point",
    "makes sense",
    "exactly",
    "true",
    "you're right",
    "youre right",
)
_CHALLENGE_PHRASES = (
    "i don't buy",
    "i dont buy",
    "doesn't make sense",
    "doesnt make sense",
    "i disagree",
    "no way",
    "not true",
    "you're wrong",
    "youre wrong",
    "that's wrong",
    "thats wrong",
)
_CHALLENGE_TOKENS = {"sus", "mafia", "lying", "lie", "wrong", "shade", "accuse", "scum", "guilty"}
_PROPOSAL_TOKENS = {"let's", "lets", "should", "vote", "plan", "choose", "pick", "decide", "go"}
_REPAIR_PHRASES = ("i mean", "wait", "sorry", "to clarify", "let me rephrase", "correction", "rather")
_SUMMARY_PHRASES = ("so ", "sounds like", "seems like", "basically", "overall", "at this point", "right now")
_NAME_TOKEN_RE = re.compile(r"\b[\w']+\b")
_NEGATION_TOKENS = {"no", "not", "never", "dont", "don't", "shouldnt", "shouldn't", "mustnt", "mustn't"}
_AFFIRMATION_TOKENS = {"yes", "correct", "locked", "confirmed", "must", "should", "keep", "preserve"}
_ARCHITECTURE_TOKENS = {"architecture", "architectural", "technical", "spec", "schema", "system", "storage", "window", "lifecycle", "constraints"}
_PRODUCT_TOKENS = {"product", "prd", "tradeoff", "tradeoffs", "scope", "requirement", "requirements", "priority", "prioritize"}
_DESIGN_TOKENS = {"design", "designer", "mockup", "mockups", "ui", "ux", "interface", "layout", "visual", "sidebar"}


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
            f"same_recent_target={snapshot.similar_recent_same_reply_target}; "
            f"same_recent_turn_kind={snapshot.similar_recent_same_turn_kind}; "
            f"similar_recent_message={snapshot.similar_recent_message_text or '(none)'}; "
            f"similar_recent_age_seconds={snapshot.similar_recent_message_age_seconds if snapshot.similar_recent_message_age_seconds is not None else '(n/a)'}; "
            f"inflight_similarity={snapshot.inflight_similarity_score:.2f}; "
            f"same_inflight_target={snapshot.similar_inflight_same_reply_target}; "
            f"same_inflight_turn_kind={snapshot.similar_inflight_same_turn_kind}; "
            f"similar_inflight_message={snapshot.similar_inflight_text or '(none)'}; "
            f"other_agents_typing={snapshot.other_agents_typing_count}"
        )
        open_questions = ", ".join(question.text_excerpt for question in snapshot.recent_open_questions[:3]) or "(none)"
        commitments = ", ".join(
            f"{commitment.polarity}:{commitment.canonical_text}"
            for commitment in snapshot.recent_commitments[:4]
        ) or "(none)"
        mafia_strategy = ""
        mafia_reactivity_guidance = ""
        if snapshot.mafia_private_state is not None and snapshot.mafia_public_state is not None:
            teammates = ", ".join(snapshot.mafia_private_state.teammates) or "(none)"
            mafia_strategy = (
                f"Current Mafia phase: {snapshot.mafia_public_state.phase.value}\n"
                f"Your private role: {snapshot.mafia_private_state.role.value}\n"
                f"Your private faction: {snapshot.mafia_private_state.faction.value}\n"
                f"Known mafia teammates: {teammates}\n"
                "This is public day chat, not a private reveal. Never mention hidden role instructions.\n"
                + (
                    "If you are mafia, favor timely public messages that sound natural, blend in, protect mafia credibility, and steer suspicion subtly.\n"
                    if snapshot.mafia_private_state.faction.value == "mafia"
                    else "If you are town, favor timely public messages that pressure suspicious behavior, compare reads, and help the table reason openly.\n"
                )
            )
        if self._config.room_mode.value == "mafia":
            mafia_reactivity_guidance = (
                "During Mafia day discussion, quick back-and-forth is healthy. Short follow-ups, piling on, and replying again after a brief beat are all normal unless you would sound like an obvious echo.\n"
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
            f"Strict turn active: {snapshot.strict_turn_active}\n"
            f"Current slot owner: {snapshot.slot_owner_id or '(none)'} / reason={snapshot.slot_reason}\n"
            f"Reply target: {snapshot.reply_target_display_name or '(none)'} / reason={snapshot.reply_target_reason}\n"
            f"Obligation strength: {snapshot.obligation_strength}\n"
            f"Floor state: {snapshot.floor_state}\n"
            f"Candidate turn kind: {snapshot.candidate_turn_kind}\n"
            f"Candidate matches active slot: {snapshot.candidate_matches_slot}\n"
            f"Candidate answers open question id: {snapshot.candidate_answers_open_question_id or '(none)'}\n"
            f"Has buffered candidate: {snapshot.has_buffered_candidate}\n"
            f"Buffered candidate preview: {candidate_preview}\n"
            f"Recent open questions: {open_questions}\n"
            f"Recent commitments: {commitments}\n"
            f"Candidate reopens a resolved question: {snapshot.candidate_reopens_resolved_question}\n"
            f"Candidate conflicts with a commitment: {snapshot.candidate_conflicts_with_commitment}\n"
            f"Candidate supports a commitment: {snapshot.candidate_supports_commitment}\n"
            f"Overlap signal: {duplicate_signal}\n"
            f"{mafia_strategy}"
            "Reason in this order: reply target, obligation to speak, available floor, turn kind, then overlap.\n"
            "Default toward `send` when your candidate is timely, relevant, and in-character.\n"
            "A high obligation means you are socially on the hook to answer, respond, defend, or acknowledge.\n"
            "If a strict response slot belongs to someone else, prefer `wait` unless your candidate is only a tiny backchannel and the room is not in strict-turn mode.\n"
            "If your candidate reopens a resolved question or conflicts with a recent accepted/rejected decision, prefer `wait`.\n"
            "If the room is quiet or nobody has started yet, sending a short opener is appropriate when your candidate is strong.\n"
            "Similar reactions are allowed when they add agreement, emotion, emphasis, support, a new target, or a different turn kind.\n"
            "Only treat overlap as a strong `wait` signal when similarity is very high and both the addressee and turn kind match a very recent or inflight competing message.\n"
            "Only prefer `wait` when one of these is true: you literally just spoke without a meaningful obligation, your candidate would be a near-duplicate echo of the same socially targeted move, or another agent is already typing essentially the same move.\n"
            "Do not over-correct for overlap: natural group chat often includes people endorsing the same option in different voices.\n"
            "Be more reactive and instinctive than cautious. A relevant, short, opinionated reply is usually better than silence.\n"
            f"{mafia_reactivity_guidance}"
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
        open_questions = "\n".join(
            f"- {question.text_excerpt}"
            for question in snapshot.recent_open_questions[:3]
        ) or "(none)"
        recent_proposals = "\n".join(
            f"- {proposal.proposer_display_name}: {proposal.canonical_text}"
            for proposal in snapshot.recent_proposals[:4]
        ) or "(none)"
        accepted_commitments = "\n".join(
            f"- {commitment.canonical_text}"
            for commitment in snapshot.accepted_commitments[:4]
        ) or "(none)"
        rejected_commitments = "\n".join(
            f"- {commitment.canonical_text}"
            for commitment in snapshot.rejected_commitments[:4]
        ) or "(none)"
        mafia_chatroom_guidance = ""
        if self._config.room_mode.value == "mafia":
            role_guidance = ""
            if snapshot.mafia_private_state is not None and snapshot.mafia_public_state is not None:
                teammates = ", ".join(snapshot.mafia_private_state.teammates) or "(none)"
                role_guidance = (
                    f"Current Mafia phase: {snapshot.mafia_public_state.phase.value}\n"
                    f"Your private role: {snapshot.mafia_private_state.role.value}\n"
                    f"Your private faction: {snapshot.mafia_private_state.faction.value}\n"
                    f"Known mafia teammates: {teammates}\n"
                    "This message is public table chat. Never reveal hidden role information or mention private instructions.\n"
                    + (
                        "If you are mafia, sound like a normal player, blend in socially, protect mafia teammates subtly, and plant doubt without being theatrical.\n"
                        if snapshot.mafia_private_state.faction.value == "mafia"
                        else "If you are town, sound like a normal town player, share honest reads, pressure suspicious behavior, and help the group reason in public.\n"
                    )
                )
            mafia_chatroom_guidance = (
                "Write like a real player in a live chat room: one short natural message, "
                "not a speech or narrator voice. No stage directions, markdown, bullet lists, "
                "scene-setting, or role labels.\n"
                f"{role_guidance}"
            )
        return (
            "Write one candidate message for a buffered asynchronous conversation.\n"
            f"You are {agent.display_name}.\n"
            f"Scenario: {snapshot.scenario}\n"
            f"Goals: {', '.join(agent.goals) or '(none)'}\n"
            f"Persona: {persona}\n"
            f"Contribution mode: {snapshot.contribution_mode}\n"
            f"Style guidance: {snapshot.style_prompt}\n"
            f"Maximum words: {snapshot.max_words}\n"
            f"Current room keywords: {topic_summary}\n"
            f"You currently own the response slot: {snapshot.owns_response_slot}\n"
            "Recent participant proposals (tentative context only):\n"
            f"{recent_proposals}\n"
            "Recent accepted decisions:\n"
            f"{accepted_commitments}\n"
            "Recent rejected decisions:\n"
            f"{rejected_commitments}\n"
            "Unresolved open questions:\n"
            f"{open_questions}\n"
            f"{mafia_chatroom_guidance}"
            "Role changes the type of contribution, not the amount of formality.\n"
            "Treat participant proposals as tentative unless they are also reflected in accepted or rejected decisions.\n"
            "Turn accepted decisions into useful next steps. Do not reopen settled tradeoffs.\n"
            "If you are translating into interface implications, avoid repetitive status chatter like 'sketching now' unless directly asked for status.\n"
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

    def mafia_vote_prompt(
        self,
        agent: AgentConfig,
        snapshot: MafiaVoteInputSnapshot,
    ) -> str:
        history = "\n".join(
            f"{message.display_name}: {message.text}" for message in snapshot.recent_messages[-6:]
        ) or "(no recent public chat)"
        roster = "\n".join(
            f"- {player.display_name} ({'alive' if player.alive else 'dead'})"
            for player in snapshot.roster
        ) or "(none)"
        reveal_summary = "\n".join(
            f"- {item.phase.value}: {item.display_name or 'nobody'} / {item.reason}"
            for item in snapshot.revealed_eliminations[-4:]
        ) or "(none)"
        return (
            "Choose one target for the current Mafia game phase.\n"
            f"You are {agent.display_name}.\n"
            f"Scenario: {snapshot.scenario}\n"
            f"Phase: {snapshot.phase.value}\n"
            f"Seconds remaining: {snapshot.seconds_remaining:.1f}\n"
            f"Your private role: {snapshot.private_state.role.value}\n"
            f"Your faction: {snapshot.private_state.faction.value}\n"
            f"Legal targets: {', '.join(snapshot.legal_targets) or '(none)'}\n"
            f"Known teammates: {', '.join(snapshot.private_state.teammates) or '(none)'}\n"
            "Public roster:\n"
            f"{roster}\n"
            "Recent public conversation:\n"
            f"{history}\n"
            "Recent reveals:\n"
            f"{reveal_summary}\n"
            "Return JSON with `target_participant_id` and `reason`."
        )


class PolicySet:
    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self.prompts = PromptPolicy(config)

    def _candidate_staleness_window(self, agent: AgentConfig, candidate: CandidateRecord) -> float:
        staleness_window = max(0.1, agent.generation.staleness_window_seconds)
        if candidate.metadata.get("mafia_lobby_spinup"):
            return max(staleness_window, 900.0)
        if candidate.metadata.get("mafia_pre_day_spinup"):
            return max(staleness_window, self._config.mafia.night_reveal_seconds + 30.0)
        return staleness_window

    @staticmethod
    def _normalize_name(value: str | None) -> str:
        return " ".join((value or "").split()).strip().casefold()

    def _message_mentions_agent(self, message, agent: AgentConfig) -> bool:
        if agent.id in message.mentions:
            return True
        display_name = self._normalize_name(agent.display_name)
        text = self._normalize_name(message.text)
        return bool(display_name and display_name in text)

    def _reply_hint_points_to_agent(
        self,
        reply_hint: str | None,
        agent: AgentConfig,
        recent_messages,
    ) -> bool:
        if not reply_hint:
            return False
        hint = self._normalize_name(reply_hint)
        if not hint:
            return False
        if hint in {self._normalize_name(agent.id), self._normalize_name(agent.display_name)}:
            return True
        for message in recent_messages:
            if message.sender_id != agent.id:
                continue
            if hint in {
                self._normalize_name(message.message_id),
                self._normalize_name(message.client_message_id),
                self._normalize_name(message.sender_id),
                self._normalize_name(message.display_name),
            }:
                return True
        return False

    def _is_question(self, text: str) -> bool:
        compact = text.strip().lower()
        if not compact:
            return False
        if "?" in compact:
            return True
        tokens = tokenize(compact)
        return bool(tokens and tokens[0] in _QUESTION_STARTERS)

    def _is_accusation(self, text: str) -> bool:
        compact = text.lower()
        if any(phrase in compact for phrase in _CHALLENGE_PHRASES):
            return True
        tokens = set(tokenize(compact))
        return bool(tokens & _CHALLENGE_TOKENS)

    def _is_direct_accusation_to_agent(self, message, agent: AgentConfig) -> bool:
        return self._message_mentions_agent(message, agent) and self._is_accusation(message.text)

    def _select_reply_target(self, agent: AgentConfig, recent_messages):
        non_self = [message for message in recent_messages if message.sender_id != agent.id]
        for message in reversed(non_self):
            if self._message_mentions_agent(message, agent):
                return message, "direct_mention"
        for message in reversed(non_self):
            if self._reply_hint_points_to_agent(message.reply_hint, agent, recent_messages):
                return message, "reply_hint"
        for message in reversed(non_self):
            if self._is_question(message.text):
                return message, "recent_question"
        if non_self:
            return non_self[-1], "recent_turn"
        return None, "none"

    def _obligation_strength(self, agent: AgentConfig, target, reason: str) -> Literal["none", "low", "medium", "high"]:
        if target is None or reason == "none":
            return "none"
        if reason == "direct_mention":
            return "high"
        if reason == "reply_hint" and (self._is_question(target.text) or self._is_direct_accusation_to_agent(target, agent)):
            return "high"
        if self._is_question(target.text) or self._is_accusation(target.text):
            return "medium"
        return "low"

    def _recent_self_cooldown_threshold(self, agent: AgentConfig) -> float:
        return max(0.12, 0.72 - (agent.personality.talkativeness * 0.45))

    def _floor_state(
        self,
        agent: AgentConfig,
        context: AgentContextSnapshot,
        obligation_strength: str,
        other_agents_typing_count: int,
        mafia_public_state: MafiaPublicState | None,
    ) -> Literal["open_floor", "addressed_response_slot", "brief_overlap_ok", "cooldown_after_self_turn"]:
        if (
            self._config.room_mode.value != "mafia"
            and context.discourse_state.strict_turn_active
            and context.discourse_state.slot_owner_id == agent.id
        ):
            return "addressed_response_slot"
        if obligation_strength == "high":
            return "addressed_response_slot"
        if context.has_sent_message and context.time_since_last_own < self._recent_self_cooldown_threshold(agent):
            return "cooldown_after_self_turn"
        if (
            self._config.room_mode.value == "mafia"
            and mafia_public_state is not None
            and mafia_public_state.phase == MafiaPhase.DAY_DISCUSSION
            and context.recent_message_count >= 2
            and (context.time_since_last_any <= 2.0 or other_agents_typing_count > 0)
        ):
            return "brief_overlap_ok"
        return "open_floor"

    def _candidate_turn_kind(
        self,
        agent: AgentConfig,
        candidate_text: str | None,
        target,
        context: AgentContextSnapshot,
    ) -> Literal["backchannel", "agreement", "answer", "challenge", "proposal", "repair", "summary", "pivot", "stance"]:
        compact = (candidate_text or "").strip().lower()
        if not compact:
            return "stance"
        token_set = set(tokenize(compact))
        words = _NAME_TOKEN_RE.findall(compact)
        if len(words) <= 4 and (token_set & _BACKCHANNEL_TOKENS):
            return "backchannel"
        if any(phrase in compact for phrase in _REPAIR_PHRASES):
            return "repair"
        if any(phrase in compact for phrase in _SUMMARY_PHRASES):
            return "summary"
        if token_set & _PROPOSAL_TOKENS or compact.startswith(("let's", "lets", "we should", "should we", "vote ", "plan ")):
            return "proposal"
        if target is not None and self._is_question(target.text) and "?" not in compact:
            return "answer"
        if any(phrase in compact for phrase in _AGREEMENT_PHRASES):
            return "agreement"
        if any(phrase in compact for phrase in _CHALLENGE_PHRASES) or (token_set & _CHALLENGE_TOKENS):
            return "challenge"
        room_keywords = {keyword.lower() for keyword in context.room_metrics.recent_keyword_sketch}
        candidate_keywords = set(tokenize(compact))
        if agent.personality.topic_loyalty < 0.5 and candidate_keywords and not (candidate_keywords & room_keywords):
            return "pivot"
        return "stance"

    def _infer_message_reply_target_id(self, message, recent_messages) -> str | None:
        if message.mentions:
            return message.mentions[0]
        hint = self._normalize_name(message.reply_hint)
        if hint:
            for candidate in recent_messages:
                if hint in {
                    self._normalize_name(candidate.message_id),
                    self._normalize_name(candidate.client_message_id),
                    self._normalize_name(candidate.sender_id),
                    self._normalize_name(candidate.display_name),
                }:
                    return candidate.sender_id
        lowered = message.text.casefold()
        for candidate in reversed(recent_messages):
            name = self._normalize_name(candidate.display_name)
            if name and name in lowered:
                return candidate.sender_id
        return None

    def _infer_turn_kind_from_text(
        self,
        text: str | None,
        recent_messages,
        reply_target_speaker_id: str | None = None,
    ) -> str:
        compact = (text or "").strip().lower()
        if not compact:
            return "stance"
        token_set = set(tokenize(compact))
        words = _NAME_TOKEN_RE.findall(compact)
        if len(words) <= 4 and (token_set & _BACKCHANNEL_TOKENS):
            return "backchannel"
        if any(phrase in compact for phrase in _REPAIR_PHRASES):
            return "repair"
        if any(phrase in compact for phrase in _SUMMARY_PHRASES):
            return "summary"
        if token_set & _PROPOSAL_TOKENS or compact.startswith(("let's", "lets", "we should", "should we", "vote ", "plan ")):
            return "proposal"
        if reply_target_speaker_id is not None:
            target_message = next((message for message in reversed(recent_messages) if message.sender_id == reply_target_speaker_id), None)
            if target_message is not None and self._is_question(target_message.text) and "?" not in compact:
                return "answer"
        if any(phrase in compact for phrase in _AGREEMENT_PHRASES):
            return "agreement"
        if any(phrase in compact for phrase in _CHALLENGE_PHRASES) or (token_set & _CHALLENGE_TOKENS):
            return "challenge"
        return "stance"

    def _recent_commitments(self, discourse_state: RoomDiscourseStateSnapshot) -> list[CommitmentState]:
        return [*discourse_state.accepted_commitments[-3:], *discourse_state.rejected_commitments[-3:]][-6:]

    def _recent_proposals(self, discourse_state: RoomDiscourseStateSnapshot) -> list[ProposalState]:
        return discourse_state.recent_proposals[-4:]

    def _text_overlap_ratio(self, tokens: set[str], keywords: list[str]) -> float:
        keyword_set = {keyword.casefold() for keyword in keywords if keyword}
        if not tokens or not keyword_set:
            return 0.0
        return len(tokens & keyword_set) / max(1, len(keyword_set))

    def _contribution_mode(
        self,
        agent: AgentConfig,
    ) -> Literal["concretize_constraints", "frame_requirements", "translate_into_interface", "generic_collaborator"]:
        goal_tokens = set(tokenize(" ".join([agent.display_name, *agent.goals, agent.style_prompt])))
        if goal_tokens & _ARCHITECTURE_TOKENS:
            return "concretize_constraints"
        if goal_tokens & _PRODUCT_TOKENS:
            return "frame_requirements"
        if goal_tokens & _DESIGN_TOKENS:
            return "translate_into_interface"
        return "generic_collaborator"

    def _candidate_answers_open_question_id(
        self,
        agent: AgentConfig,
        candidate_text: str | None,
        candidate_turn_kind: str,
        discourse_state: RoomDiscourseStateSnapshot,
        reply_target_message_id: str | None,
    ) -> str | None:
        if not candidate_text:
            return None
        candidate_tokens = set(tokenize(candidate_text))
        for question in reversed(discourse_state.open_questions):
            if question.target_participant_id not in {None, agent.id}:
                continue
            if reply_target_message_id and question.source_message_id == reply_target_message_id:
                return question.question_id
            overlap = self._text_overlap_ratio(candidate_tokens, question.keyword_sketch)
            if candidate_turn_kind in {"answer", "summary"} and overlap >= 0.25:
                return question.question_id
        return None

    def _candidate_reopens_resolved_question(
        self,
        candidate_text: str | None,
        candidate_turn_kind: str,
        discourse_state: RoomDiscourseStateSnapshot,
    ) -> bool:
        if not candidate_text:
            return False
        candidate_tokens = set(tokenize(candidate_text))
        question_like = self._is_question(candidate_text) or candidate_turn_kind in {"proposal", "pivot"} or bool(candidate_tokens & _PRODUCT_TOKENS)
        if not question_like:
            return False
        for question in reversed(discourse_state.resolved_questions):
            if self._text_overlap_ratio(candidate_tokens, question.keyword_sketch) >= 0.34:
                return True
        return False

    def _candidate_conflicts_with_commitment(
        self,
        candidate_text: str | None,
        candidate_turn_kind: str,
        discourse_state: RoomDiscourseStateSnapshot,
    ) -> bool:
        if not candidate_text:
            return False
        lowered = candidate_text.casefold()
        candidate_tokens = set(tokenize(candidate_text))
        negated = bool(candidate_tokens & _NEGATION_TOKENS) or "no context switching" in lowered
        question_like = self._is_question(candidate_text) or candidate_turn_kind in {"proposal", "pivot"} or bool(candidate_tokens & _PRODUCT_TOKENS)
        affirmative = bool(candidate_tokens & _AFFIRMATION_TOKENS) or not negated
        for commitment in discourse_state.accepted_commitments:
            if self._text_overlap_ratio(candidate_tokens, commitment.keyword_sketch) < 0.34:
                continue
            if question_like or negated:
                return True
        for commitment in discourse_state.rejected_commitments:
            if self._text_overlap_ratio(candidate_tokens, commitment.keyword_sketch) < 0.34:
                continue
            if affirmative and not negated:
                return True
        return False

    def _candidate_supports_commitment(
        self,
        candidate_text: str | None,
        discourse_state: RoomDiscourseStateSnapshot,
    ) -> bool:
        if not candidate_text:
            return False
        candidate_tokens = set(tokenize(candidate_text))
        lowered = candidate_text.casefold()
        negated = bool(candidate_tokens & _NEGATION_TOKENS) or "no context switching" in lowered
        for commitment in discourse_state.accepted_commitments:
            if self._text_overlap_ratio(candidate_tokens, commitment.keyword_sketch) >= 0.34 and not negated:
                return True
        for commitment in discourse_state.rejected_commitments:
            if self._text_overlap_ratio(candidate_tokens, commitment.keyword_sketch) >= 0.34 and negated:
                return True
        return False

    def discourse_guard_reason(
        self,
        agent: AgentConfig,
        context: AgentContextSnapshot,
        candidate: CandidateRecord,
    ) -> str | None:
        discourse_state = context.discourse_state
        if (
            self._config.room_mode.value != "mafia"
            and discourse_state.strict_turn_active
            and discourse_state.slot_owner_id not in {None, agent.id}
        ):
            return "strict_turn_slot_taken"
        candidate_turn_kind = candidate.metadata.get("candidate_turn_kind") or self._candidate_turn_kind(agent, candidate.text, None, context)
        if self._candidate_reopens_resolved_question(candidate.text, candidate_turn_kind, discourse_state):
            return "resolved_question_reopened"
        if self._candidate_conflicts_with_commitment(candidate.text, candidate_turn_kind, discourse_state):
            return "conflicts_with_commitment"
        return None

    def scheduler_input(
        self,
        agent: AgentConfig,
        context: AgentContextSnapshot,
        *,
        buffer_candidates: list[CandidateRecord] | None = None,
        active_reservations: list[DeliveryReservation] | None = None,
        mafia_public_state: MafiaPublicState | None = None,
        mafia_private_state: MafiaPrivateState | None = None,
    ) -> SchedulerInputSnapshot:
        active = max(1, context.active_participant_count)
        talk_mode: Literal["talkative", "listening"] = "talkative"
        message_share_limit = 1.0 / active
        if self._config.room_mode.value == "mafia":
            message_share_limit = 1.6 / active
        if context.agent_message_rate > message_share_limit:
            talk_mode = "listening"
        candidate_preview_text: str | None = None
        candidate_similarity_score = 0.0
        similar_recent_message_id: str | None = None
        similar_recent_message_text: str | None = None
        similar_recent_message_age_seconds: float | None = None
        similar_recent_same_reply_target = False
        similar_recent_same_turn_kind = False
        inflight_similarity_score = 0.0
        similar_inflight_text: str | None = None
        similar_inflight_same_reply_target = False
        similar_inflight_same_turn_kind = False
        reservations = list(active_reservations or [])
        other_agents_typing_count = sum(1 for reservation in reservations if reservation.agent_id != agent.id)
        reply_target, reply_target_reason = self._select_reply_target(agent, context.recent_messages)
        reply_target_message_id = reply_target.message_id if reply_target is not None else None
        reply_target_speaker_id = reply_target.sender_id if reply_target is not None else None
        reply_target_display_name = reply_target.display_name if reply_target is not None else None
        obligation_strength = self._obligation_strength(agent, reply_target, reply_target_reason)
        discourse_state = context.discourse_state
        strict_turn_active = discourse_state.strict_turn_active
        slot_owner_id = discourse_state.slot_owner_id
        slot_reason = discourse_state.slot_reason
        if self._config.room_mode.value == "mafia":
            strict_turn_active = False
            slot_owner_id = None
            slot_reason = "none"
        if strict_turn_active and slot_owner_id not in {None, agent.id}:
            obligation_strength = "none"
        floor_state = self._floor_state(
            agent,
            context,
            obligation_strength,
            other_agents_typing_count,
            mafia_public_state,
        )
        recent_open_questions = discourse_state.open_questions[-3:]
        recent_commitments = self._recent_commitments(discourse_state)
        candidate_turn_kind: Literal["backchannel", "agreement", "answer", "challenge", "proposal", "repair", "summary", "pivot", "stance"] = "stance"
        candidate_matches_slot = not strict_turn_active or slot_owner_id == agent.id
        candidate_answers_open_question_id: str | None = None
        candidate_reopens_resolved_question = False
        candidate_conflicts_with_commitment = False
        candidate_supports_commitment = False
        candidate = None
        if buffer_candidates:
            selected = self.select_best_candidate(agent, context, buffer_candidates, context.current_time)
            if selected is not None:
                candidate, _breakdown = selected
                candidate_preview_text = candidate.text
                candidate_turn_kind = self._candidate_turn_kind(agent, candidate.text, reply_target, context)
                candidate.metadata.setdefault("reply_target_speaker_id", reply_target_speaker_id)
                candidate.metadata.setdefault("candidate_turn_kind", candidate_turn_kind)
                candidate_matches_slot = not strict_turn_active or slot_owner_id == agent.id
                candidate_answers_open_question_id = self._candidate_answers_open_question_id(
                    agent,
                    candidate.text,
                    candidate_turn_kind,
                    discourse_state,
                    reply_target_message_id,
                )
                candidate_reopens_resolved_question = self._candidate_reopens_resolved_question(
                    candidate.text,
                    candidate_turn_kind,
                    discourse_state,
                )
                candidate_conflicts_with_commitment = self._candidate_conflicts_with_commitment(
                    candidate.text,
                    candidate_turn_kind,
                    discourse_state,
                )
                candidate_supports_commitment = self._candidate_supports_commitment(
                    candidate.text,
                    discourse_state,
                )
                candidate_similarity_score, similar_recent_message_text, similar_recent_message_age_seconds = (
                    self._best_recent_message_similarity(context, candidate)
                )
                inflight_similarity_score, similar_inflight_text = self._best_inflight_similarity(
                    agent,
                    reservations,
                    candidate,
                )
                similar_recent = next(
                    (message for message in context.recent_messages if message.text == similar_recent_message_text),
                    None,
                )
                if similar_recent is not None:
                    similar_recent_message_id = similar_recent.message_id
                    similar_recent_target = self._infer_message_reply_target_id(similar_recent, context.recent_messages)
                    similar_recent_same_reply_target = bool(
                        reply_target_speaker_id is not None
                        and similar_recent_target is not None
                        and similar_recent_target == reply_target_speaker_id
                    )
                    similar_recent_same_turn_kind = (
                        self._infer_turn_kind_from_text(
                            similar_recent.text,
                            context.recent_messages,
                            similar_recent_target,
                        )
                        == candidate_turn_kind
                    )
                for reservation in reservations:
                    if reservation.agent_id == agent.id or reservation.candidate.text != similar_inflight_text:
                        continue
                    inflight_target = reservation.candidate.metadata.get("reply_target_speaker_id")
                    if inflight_target is None:
                        synthetic_message = type("SyntheticMessage", (), {
                            "mentions": [],
                            "reply_hint": reservation.candidate.metadata.get("reply_hint"),
                            "text": reservation.candidate.text,
                        })()
                        inflight_target = self._infer_message_reply_target_id(synthetic_message, context.recent_messages)
                    similar_inflight_same_reply_target = bool(
                        reply_target_speaker_id is not None
                        and inflight_target is not None
                        and inflight_target == reply_target_speaker_id
                    )
                    similar_inflight_same_turn_kind = (
                        reservation.candidate.metadata.get("candidate_turn_kind")
                        or self._infer_turn_kind_from_text(
                            reservation.candidate.text,
                            context.recent_messages,
                            inflight_target if isinstance(inflight_target, str) else None,
                        )
                    ) == candidate_turn_kind
                    break
        return SchedulerInputSnapshot(
            scenario=self._config.chat.scenario,
            agent_context=context,
            goals=agent.goals,
            talk_mode=talk_mode,
            current_time_label=context.current_time.astimezone(UTC).isoformat(),
            has_buffered_candidate=bool(candidate_preview_text),
            candidate_preview_text=candidate_preview_text,
            candidate_similarity_score=candidate_similarity_score,
            similar_recent_message_id=similar_recent_message_id,
            similar_recent_message_text=similar_recent_message_text,
            similar_recent_message_age_seconds=similar_recent_message_age_seconds,
            similar_recent_same_reply_target=similar_recent_same_reply_target,
            similar_recent_same_turn_kind=similar_recent_same_turn_kind,
            inflight_similarity_score=inflight_similarity_score,
            similar_inflight_text=similar_inflight_text,
            similar_inflight_same_reply_target=similar_inflight_same_reply_target,
            similar_inflight_same_turn_kind=similar_inflight_same_turn_kind,
            other_agents_typing_count=other_agents_typing_count,
            strict_turn_active=strict_turn_active,
            slot_owner_id=slot_owner_id,
            slot_reason=slot_reason,
            reply_target_message_id=reply_target_message_id,
            reply_target_speaker_id=reply_target_speaker_id,
            reply_target_display_name=reply_target_display_name,
            reply_target_reason=reply_target_reason,
            obligation_strength=obligation_strength,
            floor_state=floor_state,
            candidate_turn_kind=candidate_turn_kind,
            candidate_matches_slot=candidate_matches_slot,
            candidate_answers_open_question_id=candidate_answers_open_question_id,
            recent_open_questions=recent_open_questions,
            recent_commitments=recent_commitments,
            candidate_reopens_resolved_question=candidate_reopens_resolved_question,
            candidate_conflicts_with_commitment=candidate_conflicts_with_commitment,
            candidate_supports_commitment=candidate_supports_commitment,
            mafia_public_state=mafia_public_state,
            mafia_private_state=mafia_private_state,
        )

    def generator_input(
        self,
        agent: AgentConfig,
        context: AgentContextSnapshot,
        mafia_public_state: MafiaPublicState | None = None,
        mafia_private_state: MafiaPrivateState | None = None,
    ) -> GeneratorInputSnapshot:
        owns_response_slot = (
            not context.discourse_state.strict_turn_active
            or context.discourse_state.slot_owner_id == agent.id
        )
        if self._config.room_mode.value == "mafia":
            owns_response_slot = True
        return GeneratorInputSnapshot(
            scenario=self._config.chat.scenario,
            agent_context=context,
            max_words=agent.max_words,
            style_prompt=agent.style_prompt,
            contribution_mode=self._contribution_mode(agent),
            owns_response_slot=owns_response_slot,
            recent_open_questions=context.discourse_state.open_questions[-3:],
            recent_proposals=self._recent_proposals(context.discourse_state),
            accepted_commitments=context.discourse_state.accepted_commitments[-4:],
            rejected_commitments=context.discourse_state.rejected_commitments[-4:],
            mafia_public_state=mafia_public_state,
            mafia_private_state=mafia_private_state,
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

    def mafia_vote_input(
        self,
        agent: AgentConfig,
        public_state: MafiaPublicState,
        private_state: MafiaPrivateState,
        recent_messages,
    ) -> MafiaVoteInputSnapshot:
        seconds_remaining = 0.0
        if public_state.phase_ends_at is not None:
            seconds_remaining = max(0.0, (public_state.phase_ends_at - datetime.now(UTC)).total_seconds())
        return MafiaVoteInputSnapshot(
            scenario=self._config.chat.scenario,
            phase=public_state.phase,
            seconds_remaining=seconds_remaining,
            roster=list(public_state.roster),
            recent_messages=list(recent_messages[-6:]),
            private_state=private_state,
            legal_targets=list(private_state.legal_targets),
            revealed_eliminations=list(public_state.revealed_eliminations),
        )

    def typing_delay(self, text: str) -> float:
        return 0.0

    def should_generate(self, context: AgentContextSnapshot, buffer_limit: int) -> bool:
        if self._config.mode == ModeProfile.BASELINE_TIME_TO_TALK:
            return False
        return context.buffer_size < buffer_limit and context.run_state == "running"

    def candidate_is_stale(self, agent: AgentConfig, candidate: CandidateRecord, now: datetime) -> bool:
        age = max(0.0, (now - candidate.created_at).total_seconds())
        staleness_window = self._candidate_staleness_window(agent, candidate)
        return age >= staleness_window

    def score_candidate(
        self,
        agent: AgentConfig,
        context: AgentContextSnapshot,
        candidate: CandidateRecord,
        now: datetime,
    ) -> tuple[float, dict[str, float]]:
        staleness_window = self._candidate_staleness_window(agent, candidate)
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
            age = max(0.0, (context.current_time - message.created_at).total_seconds())
            if age > 30.0:
                continue
            score = _text_similarity(candidate.text, message.text)
            if score <= best_score:
                continue
            best_score = score
            best_text = message.text
            best_age = age
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
