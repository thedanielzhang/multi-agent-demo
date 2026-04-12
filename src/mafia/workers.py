from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass
from datetime import timedelta
from typing import Any
from uuid import uuid4

from mafia.agent import AgentActors, AgentInvoker
from mafia.config import AgentConfig, AppConfig, ModeProfile
from mafia.messages import (
    AgentContextSnapshot,
    AgentTopicSnapshot,
    AnalyzerReply,
    AnalyzerInputSnapshot,
    CandidateRecord,
    CommandEnvelope,
    ConversationMessage,
    DeliveryReservation,
    LoggedEvent,
    GeneratorInputSnapshot,
    GeneratorReply,
    MafiaGameSnapshot,
    MafiaGameStatus,
    MafiaPhase,
    MafiaPublicState,
    MafiaPrivateState,
    MafiaVoteInputSnapshot,
    MafiaVoteReply,
    SchedulerInputSnapshot,
    SchedulerReply,
    TopicShift,
    TopicWeight,
    TransportMessagePayload,
    make_event,
    utc_now,
)
from mafia.policies import PolicySet


@dataclass(slots=True)
class WorkflowTrigger:
    trigger_kind: str
    trigger_watermark: int
    trigger_message_id: str | None = None
    trigger_sender_id: str | None = None


class BaseAgentWorker:
    def __init__(
        self,
        engine: "ConversationEngineProtocol",
        config: AppConfig,
        agent: AgentConfig,
        actors: AgentActors,
        invoker: AgentInvoker,
        policies: PolicySet,
    ) -> None:
        self.engine = engine
        self.config = config
        self.agent = agent
        self.actors = actors
        self.invoker = invoker
        self.policies = policies
        self._failures = 0

    async def _invoke(self, actor, *, prompt: str, input_data, output_type, command: CommandEnvelope | None = None):
        worker_kind = actor.role.metadata["worker_kind"]
        session_key = self.invoker.session_key_for(actor)
        workspace = self.invoker.workspace_for(actor)
        await self.engine.append_event(
            make_event(
                "debug.event.agent.call.started",
                command=command,
                payload={
                    "agent_id": self.agent.id,
                    "display_name": self.agent.display_name,
                    "worker_kind": worker_kind,
                    "actor_name": actor.name,
                    "command_subject": command.subject if command else None,
                    "session_key": session_key,
                    "workspace_path": str(workspace.path),
                    "invocation": self._invocation_metadata(command),
                    "prompt_preview": self._prompt_preview(prompt),
                    "prompt_length": len(prompt),
                    "input_summary": self._input_summary(input_data),
                },
            )
        )
        for _ in range(3):
            started = time.monotonic()
            try:
                result = await self.invoker.invoke(
                    actor,
                    prompt=prompt,
                    input_data=input_data,
                    output_type=output_type,
                )
            except Exception as exc:  # pragma: no cover - exercised through failure flow
                self._failures += 1
                await self.engine.append_event(
                    make_event(
                        "debug.event.agent.call.failed",
                        command=command,
                        payload={
                            "agent_id": self.agent.id,
                            "display_name": self.agent.display_name,
                        "worker_kind": worker_kind,
                        "actor_name": actor.name,
                        "command_subject": command.subject if command else None,
                        "session_key": session_key,
                        "workspace_path": str(workspace.path),
                        "invocation": self._invocation_metadata(command),
                        "duration_ms": round((time.monotonic() - started) * 1000, 1),
                        "error": str(exc),
                        "prompt_preview": self._prompt_preview(prompt),
                        "input_summary": self._input_summary(input_data),
                        },
                    )
                )
                await self.engine.note_worker_failure(self.agent.id, actor.role.metadata["worker_kind"], exc)
                if self.policies.consecutive_failure_is_fatal(self._failures):
                    raise
                await asyncio.sleep(0.05)
                continue
            if self.engine.registry.run_state() != "running":
                await self.engine.append_event(
                    make_event(
                        "debug.event.worker.result_dropped",
                        payload={
                            "agent_id": self.agent.id,
                            "worker_kind": actor.role.metadata["worker_kind"],
                            "reason": f"run_state={self.engine.registry.run_state()}",
                        },
                    )
                )
                return None
            self._failures = 0
            await self.engine.append_event(
                make_event(
                    "debug.event.agent.call.completed",
                    command=command,
                    payload={
                        "agent_id": self.agent.id,
                        "display_name": self.agent.display_name,
                        "worker_kind": worker_kind,
                        "actor_name": actor.name,
                        "command_subject": command.subject if command else None,
                        "session_key": session_key,
                        "workspace_path": str(workspace.path),
                        "invocation": self._invocation_metadata(command),
                        "duration_ms": round((time.monotonic() - started) * 1000, 1),
                        "prompt_preview": self._prompt_preview(prompt),
                        "input_summary": self._input_summary(input_data),
                        "output_summary": self._output_summary(result),
                    },
                )
            )
            await self.engine.note_worker_success(self.agent.id, actor.role.metadata["worker_kind"])
            return result
        raise RuntimeError(f"{actor.name} failed three times")

    def _candidate_from_context(self, context, text: str) -> CandidateRecord:
        dominant_topic_id = context.topic_snapshot.dominant_topic_id if context.topic_snapshot else None
        generation_keywords = list(context.room_metrics.recent_keyword_sketch)
        topic_weights: list[TopicWeight] = []
        if context.topic_snapshot:
            topic_weights = [
                TopicWeight(topic_id=topic.topic_id, weight=topic.weight)
                for topic in context.topic_snapshot.topics
            ]
            for topic in context.topic_snapshot.topics:
                if topic.topic_id == dominant_topic_id:
                    generation_keywords = list(topic.keywords)
                    break
        return CandidateRecord(
            candidate_id=str(uuid4()),
            agent_id=self.agent.id,
            text=text,
            created_at=utc_now(),
            source_watermark=context.watermark,
            topic_snapshot_id=context.topic_snapshot_id,
            dominant_topic_id_at_generation=dominant_topic_id,
            generation_keywords=generation_keywords,
            focus_message_ids=list(context.focus_message_ids),
            topic_weights=topic_weights,
            buffer_version=self.engine.registry.buffer_version_for(self.agent.id),
        )

    def _normalize_generated_text(self, text: str) -> str:
        candidate = text.strip()
        if candidate.startswith("```"):
            lines = candidate.splitlines()
            if len(lines) >= 2:
                lines = lines[1:]
                if lines and lines[-1].strip().startswith("```"):
                    lines = lines[:-1]
                candidate = "\n".join(lines).strip()
        for payload in (candidate, text.strip()):
            if not payload:
                continue
            try:
                decoded = json.loads(payload)
            except json.JSONDecodeError:
                continue
            if isinstance(decoded, dict):
                nested = decoded.get("text")
                if isinstance(nested, str) and nested.strip():
                    return nested.strip()
            if isinstance(decoded, str) and decoded.strip():
                return decoded.strip()
        return candidate or text.strip()

    def _reservation_payload(self, candidate: CandidateRecord, client_message_id: str) -> DeliveryReservation:
        return DeliveryReservation(
            reservation_id=str(uuid4()),
            agent_id=self.agent.id,
            candidate=candidate,
            client_message_id=client_message_id,
            created_at=utc_now(),
        )

    def _invocation_metadata(self, command: CommandEnvelope | None) -> dict[str, object]:
        payload = command.payload if command else {}
        return {
            "workflow": bool(payload.get("workflow", False)),
            "trigger_kind": payload.get("trigger_kind"),
            "trigger_watermark": payload.get("trigger_watermark"),
            "trigger_message_id": payload.get("trigger_message_id"),
            "mafia_lobby_spinup": bool(payload.get("mafia_lobby_spinup", False)),
            "mafia_pre_day_spinup": bool(payload.get("mafia_pre_day_spinup", False)),
            "reactive": bool(payload.get("reactive", False)),
            "heartbeat": bool(payload.get("heartbeat", False)),
            "bootstrap": bool(payload.get("bootstrap", False)),
            "follow_up": int(payload.get("follow_up", 0) or 0),
            "message_id": payload.get("message_id"),
        }

    def _mafia_chat_state(self) -> tuple[MafiaPublicState | None, MafiaPrivateState | None]:
        if self.config.room_mode.value != "mafia":
            return None, None
        return self.engine.registry.mafia_public_state(), self.engine.registry.mafia_private_state_for(self.agent.id)

    def _prompt_preview(self, prompt: str, limit: int = 500) -> str:
        compact = " ".join(prompt.split())
        if len(compact) <= limit:
            return compact
        return compact[: limit - 3] + "..."

    def _input_summary(self, input_data) -> dict[str, object]:
        if isinstance(input_data, SchedulerInputSnapshot):
            context = input_data.agent_context
            summary = {
                "scenario": input_data.scenario,
                "talk_mode": input_data.talk_mode,
                "watermark": context.watermark,
                "room_is_idle": context.room_is_idle,
                "has_sent_message": context.has_sent_message,
                "own_message_count": context.own_message_count,
                "focus_count": len(context.focus_messages),
                "buffer_size": context.buffer_size,
                "candidate_preview_text": input_data.candidate_preview_text,
                "candidate_similarity_score": round(input_data.candidate_similarity_score, 3),
                "similar_recent_message_text": input_data.similar_recent_message_text,
                "inflight_similarity_score": round(input_data.inflight_similarity_score, 3),
                "other_agents_typing_count": input_data.other_agents_typing_count,
                "time_since_last_any": round(context.time_since_last_any, 3),
                "time_since_last_own": round(context.time_since_last_own, 3),
                "room_keywords": list(context.room_metrics.recent_keyword_sketch),
            }
            if input_data.mafia_private_state is not None and input_data.mafia_public_state is not None:
                summary.update(
                    {
                        "mafia_phase": input_data.mafia_public_state.phase.value,
                        "role": input_data.mafia_private_state.role.value,
                        "faction": input_data.mafia_private_state.faction.value,
                    }
                )
            return summary
        if isinstance(input_data, GeneratorInputSnapshot):
            context = input_data.agent_context
            summary = {
                "scenario": input_data.scenario,
                "watermark": context.watermark,
                "focus_count": len(context.focus_messages),
                "focus_preview": [message.text for message in context.focus_messages[-3:]],
                "room_keywords": list(context.room_metrics.recent_keyword_sketch),
                "topic_snapshot_id": context.topic_snapshot_id,
                "max_words": input_data.max_words,
                "style_prompt": input_data.style_prompt,
            }
            if input_data.mafia_private_state is not None and input_data.mafia_public_state is not None:
                summary.update(
                    {
                        "mafia_phase": input_data.mafia_public_state.phase.value,
                        "role": input_data.mafia_private_state.role.value,
                        "faction": input_data.mafia_private_state.faction.value,
                    }
                )
            return summary
        if isinstance(input_data, AnalyzerInputSnapshot):
            return {
                "scenario": input_data.scenario,
                "message_count": len(input_data.recent_messages),
                "message_preview": [message.text for message in input_data.recent_messages[-4:]],
                "seed_topics": list(input_data.seed_topics),
                "previous_snapshot_id": input_data.previous_snapshot.snapshot_id if input_data.previous_snapshot else None,
            }
        if isinstance(input_data, MafiaVoteInputSnapshot):
            return {
                "scenario": input_data.scenario,
                "phase": input_data.phase.value,
                "seconds_remaining": round(input_data.seconds_remaining, 1),
                "legal_targets": list(input_data.legal_targets),
                "role": input_data.private_state.role.value,
                "faction": input_data.private_state.faction.value,
                "recent_preview": [message.text for message in input_data.recent_messages[-3:]],
            }
        if isinstance(input_data, AgentContextSnapshot):
            return {
                "watermark": input_data.watermark,
                "focus_count": len(input_data.focus_messages),
                "buffer_size": input_data.buffer_size,
            }
        return {}

    def _output_summary(self, result) -> dict[str, object]:
        if isinstance(result, SchedulerReply):
            return {
                "decision": result.decision,
                "reason": result.reason,
            }
        if isinstance(result, GeneratorReply):
            return {
                "text": result.text,
                "word_count": len(result.text.split()),
            }
        if isinstance(result, AnalyzerReply):
            return {
                "topics": [
                    {
                        "label": topic.label,
                        "topic_id": topic.topic_id,
                        "keywords": list(topic.keywords),
                    }
                    for topic in result.topics
                ],
            }
        if isinstance(result, MafiaVoteReply):
            return {
                "target_participant_id": result.target_participant_id,
                "reason": result.reason,
            }
        return {"type": type(result).__name__}


class AgentTopicAnalyzerWorker(BaseAgentWorker):
    async def handle_tick(self, command: CommandEnvelope) -> None:
        await self.run_step(command=command)

    async def run_step(
        self,
        *,
        command: CommandEnvelope | None = None,
        context: AgentContextSnapshot | None = None,
    ) -> AgentTopicSnapshot | None:
        if self.config.mode == ModeProfile.BASELINE_TIME_TO_TALK or not self.config.topic.enabled:
            return None
        context = context or self.engine.registry.agent_view(self.agent)
        previous = context.topic_snapshot
        analyzer_input = self.policies.analyzer_input(self.agent, context, previous)
        reply = await self._invoke(
            self.actors.analyzer,
            prompt=self.policies.prompts.analyzer_prompt(analyzer_input),
            input_data=analyzer_input,
            output_type=AnalyzerReply,
            command=command,
        )
        if reply is None:
            return
        topics, message_topics, memory_summary = self.policies.reconcile_topics(self.agent, previous, reply)
        now = utc_now()
        dominant = topics[0].topic_id if topics else None
        previous_topic = previous.dominant_topic_id if previous else None
        snapshot = AgentTopicSnapshot(
            snapshot_id=str(uuid4()),
            agent_id=self.agent.id,
            watermark=context.watermark,
            window_message_ids=list(context.focus_message_ids),
            topics=topics[: self.config.topic.max_topics],
            message_topics=message_topics,
            dominant_topic_id=dominant,
            shift=TopicShift(
                previous_topic_id=previous_topic,
                current_topic_id=dominant,
                shifted=bool(previous_topic and dominant and previous_topic != dominant),
            ),
            generated_at=now,
            stale_after=now + timedelta(seconds=self.config.topic.stale_after_seconds),
            memory_summary=memory_summary,
        )
        await self.engine.append_event_and_wait(
            make_event(
                f"topic.event.{self.agent.id}.snapshot.updated",
                command=command,
                payload=snapshot,
            )
        )
        return snapshot


class AgentGenerationWorker(BaseAgentWorker):
    async def handle_tick(self, command: CommandEnvelope) -> None:
        await self.run_step(command=command, enqueue_schedule=True)

    async def run_step(
        self,
        *,
        command: CommandEnvelope | None = None,
        context: AgentContextSnapshot | None = None,
        enqueue_schedule: bool = False,
    ) -> CandidateRecord | None:
        context = context or self.engine.registry.agent_view(self.agent)
        payload = command.payload if command else {}
        reactive_refresh = bool(payload.get("reactive"))
        lobby_spinup = bool(payload.get("mafia_lobby_spinup"))
        pre_day_spinup = bool(payload.get("mafia_pre_day_spinup"))
        if not self.policies.should_generate(context, self.agent.generation.buffer_size):
            if not reactive_refresh or self.config.mode != ModeProfile.IMPROVED_BUFFERED_ASYNC:
                return None
            buffer = self.engine.registry.buffer_for(self.agent.id)
            if not buffer:
                return None
            newest_source = max(candidate.source_watermark for candidate in buffer)
            if newest_source >= context.watermark:
                return None
            oldest = min(buffer, key=lambda candidate: (candidate.created_at, candidate.candidate_id))
            await self.engine.append_event(
                make_event(
                    f"agent.event.{self.agent.id}.candidate.discarded",
                    command=command,
                    payload={
                        "agent_id": self.agent.id,
                        "candidate_id": oldest.candidate_id,
                        "reason": "reactive_refresh",
                    },
                )
            )
        mafia_public_state, mafia_private_state = self._mafia_chat_state()
        if lobby_spinup:
            mafia_public_state = None
            mafia_private_state = None
        generator_input = self.policies.generator_input(
            self.agent,
            context,
            mafia_public_state=mafia_public_state,
            mafia_private_state=mafia_private_state,
        )
        reply = await self._invoke(
            self.actors.generator,
            prompt=self.policies.prompts.generator_prompt(self.agent, generator_input),
            input_data=generator_input,
            output_type=GeneratorReply,
            command=command,
        )
        if reply is None:
            return
        candidate = self._candidate_from_context(context, self._normalize_generated_text(reply.text))
        if lobby_spinup:
            candidate.metadata["mafia_lobby_spinup"] = True
        if pre_day_spinup:
            candidate.metadata["mafia_pre_day_spinup"] = True
        await self.engine.append_event(
            make_event(
                f"agent.event.{self.agent.id}.candidate.generated",
                command=command,
                payload=candidate,
            )
        )
        await self.engine.append_event_and_wait(
            make_event(
                f"agent.event.{self.agent.id}.candidate.buffered",
                command=command,
                payload=candidate,
            )
        )
        if (
            enqueue_schedule
            and self.config.mode == ModeProfile.IMPROVED_BUFFERED_ASYNC
            and self.engine.registry.run_state() == "running"
        ):
            await self.engine.enqueue_command(
                CommandEnvelope(
                    subject=f"agent.command.{self.agent.id}.schedule.tick",
                    correlation_id=command.correlation_id,
                    causation_id=command.command_id,
                    payload={
                        "buffer_ready": True,
                        "reactive": bool(payload.get("reactive", False)),
                        "follow_up": int(payload.get("follow_up", 0) or 0),
                        "message_id": payload.get("message_id"),
                    },
                )
            )
        return candidate


class AgentBufferWorker(BaseAgentWorker):
    async def handle_evict_tick(self, command: CommandEnvelope) -> None:
        await self.discard_stale(command=command)

    async def discard_stale(self, *, command: CommandEnvelope | None = None) -> int:
        if self.config.mode != ModeProfile.IMPROVED_BUFFERED_ASYNC:
            return 0
        now = utc_now()
        discarded = 0
        for candidate in self.engine.registry.buffer_for(self.agent.id):
            if not self.policies.candidate_is_stale(self.agent, candidate, now):
                continue
            discarded += 1
            await self.engine.append_event(
                make_event(
                    f"agent.event.{self.agent.id}.candidate.discarded",
                    command=command,
                    payload={
                        "agent_id": self.agent.id,
                        "candidate_id": candidate.candidate_id,
                        "reason": "stale",
                    },
                )
            )
        return discarded


class AgentSchedulerWorker(BaseAgentWorker):
    async def handle_tick(self, command: CommandEnvelope) -> None:
        await self.run_step(command=command)

    async def run_step(
        self,
        *,
        command: CommandEnvelope | None = None,
        context: AgentContextSnapshot | None = None,
    ) -> SchedulerReply | None:
        context = context or self.engine.registry.agent_view(self.agent)
        buffer_candidates = self.engine.registry.buffer_for(self.agent.id)
        active_reservations = self.engine.registry.active_reservations()
        mafia_public_state, mafia_private_state = self._mafia_chat_state()
        scheduler_input = self.policies.scheduler_input(
            self.agent,
            context,
            buffer_candidates=buffer_candidates,
            active_reservations=active_reservations,
            mafia_public_state=mafia_public_state,
            mafia_private_state=mafia_private_state,
        )
        if self.engine.registry.active_reservation_for(self.agent.id) is not None:
            await self.engine.append_event(
                make_event(
                    f"agent.event.{self.agent.id}.scheduler.decided",
                    command=command,
                    payload={
                        "agent_id": self.agent.id,
                        "decision": "wait",
                        "reason": "active_reservation",
                        "buffer_size": context.buffer_size,
                        "source_watermark": context.watermark,
                        "source_topic_snapshot_id": context.topic_snapshot_id,
                        "talk_mode": scheduler_input.talk_mode,
                    },
                )
            )
            return SchedulerReply(decision="wait", reason="active_reservation")
        if self.config.mode == ModeProfile.IMPROVED_BUFFERED_ASYNC and not buffer_candidates:
            await self.engine.append_event(
                make_event(
                    f"agent.event.{self.agent.id}.scheduler.decided",
                    command=command,
                    payload={
                        "agent_id": self.agent.id,
                        "decision": "wait",
                        "reason": "no-buffered-candidate",
                        "buffer_size": context.buffer_size,
                        "source_watermark": context.watermark,
                        "source_topic_snapshot_id": context.topic_snapshot_id,
                        "talk_mode": scheduler_input.talk_mode,
                    },
                )
            )
            return SchedulerReply(decision="wait", reason="no-buffered-candidate")
        reply = await self._invoke(
            self.actors.scheduler,
            prompt=self.policies.prompts.scheduler_prompt(self.agent, scheduler_input),
            input_data=scheduler_input,
            output_type=SchedulerReply,
            command=command,
        )
        if reply is None:
            return
        await self.engine.append_event(
            make_event(
                f"agent.event.{self.agent.id}.scheduler.decided",
                command=command,
                payload={
                    "agent_id": self.agent.id,
                    "decision": reply.decision,
                    "reason": reply.reason,
                    "buffer_size": context.buffer_size,
                    "source_watermark": context.watermark,
                    "source_topic_snapshot_id": context.topic_snapshot_id,
                    "talk_mode": scheduler_input.talk_mode,
                },
            )
        )
        if reply.decision != "send":
            return reply
        latest_context = self.engine.registry.agent_view(self.agent)
        if self.config.mode == ModeProfile.BASELINE_TIME_TO_TALK:
            await self._send_baseline_candidate(command, latest_context)
        else:
            await self._send_buffered_candidate(command, latest_context)
        return reply

    async def _send_baseline_candidate(self, command: CommandEnvelope, context) -> None:
        mafia_public_state, mafia_private_state = self._mafia_chat_state()
        generator_input = self.policies.generator_input(
            self.agent,
            context,
            mafia_public_state=mafia_public_state,
            mafia_private_state=mafia_private_state,
        )
        reply = await self._invoke(
            self.actors.generator,
            prompt=self.policies.prompts.generator_prompt(self.agent, generator_input),
            input_data=generator_input,
            output_type=GeneratorReply,
            command=command,
        )
        if reply is None:
            return
        candidate = self._candidate_from_context(context, self._normalize_generated_text(reply.text))
        await self.engine.append_event(
            make_event(
                f"agent.event.{self.agent.id}.candidate.generated",
                command=command,
                payload=candidate,
            )
        )
        reservation = self._reservation_payload(candidate, client_message_id=str(uuid4()))
        await self.engine.append_event_and_wait(
            make_event(
                f"agent.event.{self.agent.id}.candidate.reserved",
                command=command,
                payload=reservation,
            )
        )
        await self.engine.enqueue_command(
            CommandEnvelope(
                subject=f"agent.command.{self.agent.id}.deliver.request",
                correlation_id=command.correlation_id,
                causation_id=command.command_id,
                payload={
                    "agent_id": self.agent.id,
                    "reservation_id": reservation.reservation_id,
                },
            )
        )

    async def _send_buffered_candidate(self, command: CommandEnvelope, context) -> None:
        candidates = await self._fresh_send_candidates(command=command)
        if not candidates:
            regenerated = await self._regenerate_send_candidate(command=command)
            if regenerated is None:
                return
            context = self.engine.registry.agent_view(self.agent)
            candidates = [regenerated]
        if not candidates:
            return
        selected = self.policies.select_best_candidate(self.agent, context, candidates, utc_now())
        if selected is None:
            return
        best, breakdown = selected
        reservation = self._reservation_payload(best, client_message_id=str(uuid4()))
        await self.engine.append_event_and_wait(
            make_event(
                f"agent.event.{self.agent.id}.candidate.reserved",
                command=command,
                payload={
                    **reservation.model_dump(mode="json"),
                    "candidate_id": best.candidate_id,
                    "agent_id": self.agent.id,
                    "score_breakdown": breakdown,
                },
            )
        )
        await self.engine.enqueue_command(
            CommandEnvelope(
                subject=f"agent.command.{self.agent.id}.deliver.request",
                correlation_id=command.correlation_id,
                causation_id=command.command_id,
                payload={
                    "agent_id": self.agent.id,
                    "reservation_id": reservation.reservation_id,
                },
            )
        )

    async def _fresh_send_candidates(self, *, command: CommandEnvelope) -> list[CandidateRecord]:
        now = utc_now()
        fresh: list[CandidateRecord] = []
        for candidate in self.engine.registry.buffer_for(self.agent.id):
            if not self.policies.candidate_is_stale(self.agent, candidate, now):
                fresh.append(candidate)
                continue
            await self.engine.append_event(
                make_event(
                    f"agent.event.{self.agent.id}.candidate.discarded",
                    command=command,
                    payload={
                        "agent_id": self.agent.id,
                        "candidate_id": candidate.candidate_id,
                        "reason": "stale_before_send",
                    },
                )
            )
        return fresh

    async def _regenerate_send_candidate(self, *, command: CommandEnvelope) -> CandidateRecord | None:
        context = self.engine.registry.agent_view(self.agent)
        if self.engine.registry.run_state() != "running":
            return None
        mafia_public_state, mafia_private_state = self._mafia_chat_state()
        generator_input = self.policies.generator_input(
            self.agent,
            context,
            mafia_public_state=mafia_public_state,
            mafia_private_state=mafia_private_state,
        )
        reply = await self._invoke(
            self.actors.generator,
            prompt=self.policies.prompts.generator_prompt(self.agent, generator_input),
            input_data=generator_input,
            output_type=GeneratorReply,
            command=command,
        )
        if reply is None:
            return None
        candidate = self._candidate_from_context(context, self._normalize_generated_text(reply.text))
        await self.engine.append_event(
            make_event(
                f"agent.event.{self.agent.id}.candidate.generated",
                command=command,
                payload=candidate,
            )
        )
        await self.engine.append_event_and_wait(
            make_event(
                f"agent.event.{self.agent.id}.candidate.buffered",
                command=command,
                payload=candidate,
            )
        )
        return candidate


class MafiaVoteWorker(BaseAgentWorker):
    async def handle_vote(self, command: CommandEnvelope) -> None:
        if self.config.room_mode.value != "mafia":
            return
        public_state = self.engine.registry.mafia_public_state()
        private_state = self.engine.registry.mafia_private_state_for(self.agent.id)
        if public_state is None or private_state is None:
            return
        if public_state.game_status != MafiaGameStatus.ACTIVE:
            return
        if public_state.phase not in {MafiaPhase.DAY_VOTE, MafiaPhase.NIGHT_ACTION}:
            return
        if not (private_state.can_vote or private_state.can_act):
            return
        input_data = self.policies.mafia_vote_input(
            self.agent,
            public_state,
            private_state,
            self.engine.registry.latest_messages(),
        )
        reply = await self._invoke(
            self.actors.voter,
            prompt=self.policies.prompts.mafia_vote_prompt(self.agent, input_data),
            input_data=input_data,
            output_type=MafiaVoteReply,
            command=command,
        )
        if reply is None:
            return
        target = reply.target_participant_id
        if target is not None and target not in private_state.legal_targets:
            target = None
        await self.engine.dispatch_command(
            CommandEnvelope(
                subject="mafia.command.vote.cast",
                correlation_id=command.correlation_id,
                causation_id=command.command_id,
                payload={
                    "participant_id": self.agent.id,
                    "target_participant_id": target,
                },
            )
        )


class AgentDeliveryWorker:
    def __init__(self, engine: "ConversationEngineProtocol", config: AppConfig, agent: AgentConfig) -> None:
        self.engine = engine
        self.config = config
        self.agent = agent

    async def handle_request(self, command: CommandEnvelope) -> None:
        reservation = self.engine.registry.reservation_for(command.payload["reservation_id"])
        if reservation is None:
            await self._abort(command, reservation_id=command.payload["reservation_id"], candidate_id=None, reason="unknown_reservation")
            return
        if not self._mafia_chat_open():
            await self._abort(command, reservation_id=reservation.reservation_id, candidate_id=reservation.candidate.candidate_id, reason="mafia_chat_closed")
            return
        if self.engine.policies.candidate_is_stale(self.agent, reservation.candidate, utc_now()):
            await self._abort(command, reservation_id=reservation.reservation_id, candidate_id=reservation.candidate.candidate_id, reason="reservation_expired")
            return
        delay = self.engine.policies.typing_delay(reservation.candidate.text)
        await self.engine.append_event(
            make_event(
                f"agent.event.{self.agent.id}.delivery.scheduled",
                command=command,
                payload={
                    "agent_id": self.agent.id,
                    "reservation_id": reservation.reservation_id,
                    "candidate_id": reservation.candidate.candidate_id,
                    "delay_seconds": delay,
                },
            )
        )
        submit_command = CommandEnvelope(
            subject=f"agent.command.{self.agent.id}.deliver.submit",
            correlation_id=command.correlation_id,
            causation_id=command.command_id,
            payload={
                "agent_id": self.agent.id,
                "reservation_id": reservation.reservation_id,
            },
        )
        if delay <= 0:
            await self.engine.dispatch_command(submit_command)
            return
        await self.engine.clock.schedule_delivery(delay, submit_command)

    async def handle_submit(self, command: CommandEnvelope) -> None:
        reservation = self.engine.registry.reservation_for(command.payload["reservation_id"])
        if reservation is None:
            return
        if not self._mafia_chat_open():
            await self._abort(command, reservation_id=reservation.reservation_id, candidate_id=reservation.candidate.candidate_id, reason="mafia_chat_closed")
            return
        if self.engine.policies.candidate_is_stale(self.agent, reservation.candidate, utc_now()):
            await self._abort(command, reservation_id=reservation.reservation_id, candidate_id=reservation.candidate.candidate_id, reason="reservation_expired")
            return
        await self.engine.append_event(
            make_event(
                f"agent.event.{self.agent.id}.delivery.submitted",
                command=command,
                payload={
                    "agent_id": self.agent.id,
                    "reservation_id": reservation.reservation_id,
                    "candidate_id": reservation.candidate.candidate_id,
                },
            )
        )
        await self.engine.dispatch_command(
            CommandEnvelope(
                subject="transport.command.message.send",
                correlation_id=command.correlation_id,
                causation_id=command.command_id,
                payload=TransportMessagePayload(
                    reservation_id=reservation.reservation_id,
                    candidate_id=reservation.candidate.candidate_id,
                    client_message_id=reservation.client_message_id,
                    agent_id=self.agent.id,
                    sender_id=self.agent.id,
                    sender_kind="agent",
                    display_name=self.agent.display_name,
                    text=reservation.candidate.text,
                    metadata={
                        "candidate_id": reservation.candidate.candidate_id,
                        "reservation_id": reservation.reservation_id,
                    },
                ).model_dump(mode="json"),
            )
        )

    async def handle_transport_acked(self, command: CommandEnvelope) -> None:
        await self.engine.append_event(
            make_event(
                f"agent.event.{self.agent.id}.delivery.acked",
                command=command,
                payload={
                    "agent_id": self.agent.id,
                    "reservation_id": command.payload["reservation_id"],
                    "candidate_id": command.payload["candidate_id"],
                    "message_id": command.payload["message_id"],
                },
            )
        )

    async def handle_transport_failed(self, command: CommandEnvelope) -> None:
        reservation = self.engine.registry.reservation_for(command.payload["reservation_id"])
        if reservation is None:
            return
        retryable = bool(command.payload.get("retryable", False))
        if retryable and not self.engine.policies.candidate_is_stale(self.agent, reservation.candidate, utc_now()):
            await self.engine.append_event(
                make_event(
                    f"agent.event.{self.agent.id}.candidate.requeued",
                    command=command,
                    payload={
                        "reservation_id": reservation.reservation_id,
                        "candidate": reservation.candidate,
                        "reason": command.payload.get("reason", "transport_retryable_failure"),
                    },
                )
            )
            return
        await self._abort(
            command,
            reservation_id=reservation.reservation_id,
            candidate_id=reservation.candidate.candidate_id,
            reason=command.payload.get("reason", "transport_failure"),
        )

    async def _abort(self, command: CommandEnvelope, *, reservation_id: str, candidate_id: str | None, reason: str) -> None:
        await self.engine.append_event(
            make_event(
                f"agent.event.{self.agent.id}.delivery.aborted",
                command=command,
                payload={
                    "agent_id": self.agent.id,
                    "reservation_id": reservation_id,
                    "candidate_id": candidate_id,
                    "reason": reason,
                },
            )
        )

    def _mafia_chat_open(self) -> bool:
        if self.config.room_mode.value != "mafia":
            return True
        private_state = self.engine.registry.mafia_private_state_for(self.agent.id)
        return bool(private_state and private_state.can_chat)


class ImprovedAgentWorkflowRunner:
    def __init__(
        self,
        engine: "ConversationEngineProtocol",
        config: AppConfig,
        agent: AgentConfig,
        analyzer: AgentTopicAnalyzerWorker,
        generator: AgentGenerationWorker,
        buffer_worker: AgentBufferWorker,
        scheduler: AgentSchedulerWorker,
    ) -> None:
        self.engine = engine
        self.config = config
        self.agent = agent
        self.analyzer = analyzer
        self.generator = generator
        self.buffer_worker = buffer_worker
        self.scheduler = scheduler
        self._pending: WorkflowTrigger | None = None
        self._task: asyncio.Task[None] | None = None
        self._follow_up_task: asyncio.Task[None] | None = None
        self._analysis_task: asyncio.Task[None] | None = None
        self._closed = False

    async def close(self) -> None:
        self._closed = True
        if self._analysis_task:
            self._analysis_task.cancel()
        if self._follow_up_task:
            self._follow_up_task.cancel()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        if self._analysis_task:
            await asyncio.gather(self._analysis_task, return_exceptions=True)
        if self._follow_up_task:
            await asyncio.gather(self._follow_up_task, return_exceptions=True)

    async def on_logged_event(self, logged_event: LoggedEvent) -> None:
        subject = logged_event.event.subject
        if subject == "conversation.event.message.committed":
            message = ConversationMessage.model_validate(logged_event.event.payload)
            await self.notify(
                WorkflowTrigger(
                    trigger_kind="room_message",
                    trigger_watermark=logged_event.seq,
                    trigger_message_id=message.message_id,
                    trigger_sender_id=message.sender_id,
                )
            )
            return
        if subject == "mafia.event.snapshot.updated":
            snapshot = MafiaGameSnapshot.model_validate(logged_event.event.payload)
            if snapshot.phase == MafiaPhase.LOBBY:
                await self.notify(
                    WorkflowTrigger(
                        trigger_kind="mafia_lobby",
                        trigger_watermark=logged_event.seq,
                    )
                )
                return
            if snapshot.game_status == MafiaGameStatus.ACTIVE and snapshot.phase == MafiaPhase.NIGHT_REVEAL:
                await self.notify(
                    WorkflowTrigger(
                        trigger_kind="mafia_pre_day",
                        trigger_watermark=logged_event.seq,
                    )
                )
                return
            if snapshot.game_status == MafiaGameStatus.ACTIVE and snapshot.phase == MafiaPhase.DAY_DISCUSSION:
                await self.notify(
                    WorkflowTrigger(
                        trigger_kind="mafia_phase",
                        trigger_watermark=logged_event.seq,
                    )
                )
                return
        if subject == "run.event.state.changed" and logged_event.event.payload.get("state") == "running":
            await self.notify(
                WorkflowTrigger(
                    trigger_kind="run_start",
                    trigger_watermark=logged_event.seq,
                )
            )

    async def notify(self, trigger: WorkflowTrigger) -> None:
        if self._closed:
            return
        self._cancel_follow_up()
        if self._task and not self._task.done():
            self._pending = trigger
            await self._emit(
                "debug.event.agent.workflow.coalesced",
                trigger=trigger,
                effective_watermark=max(trigger.trigger_watermark, self.engine.registry.watermark),
                rerun_pending=True,
            )
            return
        self._pending = trigger
        self._task = self.engine.create_background_task(
            self._drain(),
            f"workflow.{self.agent.id}",
        )

    async def _drain(self) -> None:
        try:
            while not self._closed and self._pending is not None:
                trigger = self._pending
                self._pending = None
                await self._run_once(trigger)
        finally:
            self._task = None

    async def _run_once(self, trigger: WorkflowTrigger) -> None:
        effective_watermark = max(trigger.trigger_watermark, self.engine.registry.watermark)
        await self._emit(
            "debug.event.agent.workflow.started",
            trigger=trigger,
            effective_watermark=effective_watermark,
            rerun_pending=False,
        )
        if self.engine.registry.run_state() != "running":
            await self._emit(
                "debug.event.agent.workflow.skipped",
                trigger=trigger,
                effective_watermark=effective_watermark,
                rerun_pending=bool(self._pending),
                reason=f"run_state={self.engine.registry.run_state()}",
            )
            return
        if trigger.trigger_kind == "room_message" and trigger.trigger_sender_id == self.agent.id:
            await self._emit(
                "debug.event.agent.workflow.skipped",
                trigger=trigger,
                effective_watermark=effective_watermark,
                rerun_pending=bool(self._pending),
                reason="self_message",
            )
            return
        if self.config.room_mode.value == "mafia":
            private_state = self.engine.registry.mafia_private_state_for(self.agent.id)
            public_state = self.engine.registry.mafia_public_state()
            if public_state is None:
                await self._emit(
                    "debug.event.agent.workflow.skipped",
                    trigger=trigger,
                    effective_watermark=effective_watermark,
                    rerun_pending=bool(self._pending),
                    reason="mafia_state_unavailable",
                )
                return
            if public_state.phase == MafiaPhase.LOBBY:
                await self._run_mafia_spinup(
                    trigger,
                    reason="mafia_lobby_spinup",
                    extra_payload={"mafia_lobby_spinup": True},
                )
                return
            if public_state.phase == MafiaPhase.NIGHT_REVEAL:
                await self._run_mafia_spinup(
                    trigger,
                    reason="mafia_pre_day_spinup",
                    extra_payload={"mafia_pre_day_spinup": True, "reactive": True},
                )
                return
            if private_state is None or not private_state.can_chat:
                await self._emit(
                    "debug.event.agent.workflow.skipped",
                    trigger=trigger,
                    effective_watermark=effective_watermark,
                    rerun_pending=bool(self._pending),
                    reason="mafia_chat_disabled",
                )
                return

        generated_candidate: dict[str, object] | None = None
        scheduler_decision: dict[str, object] | None = None

        context = self.engine.registry.agent_view(self.agent)
        await self.buffer_worker.discard_stale(command=self._workflow_command("evict", trigger))
        context = self.engine.registry.agent_view(self.agent)

        if not self.engine.registry.buffer_for(self.agent.id):
            candidate = await self.generator.run_step(
                command=self._workflow_command("generate", trigger),
                context=context,
                enqueue_schedule=False,
            )
            if candidate is not None:
                generated_candidate = {
                    "candidate_id": candidate.candidate_id,
                    "text": candidate.text,
                    "source_watermark": candidate.source_watermark,
                }
            context = self.engine.registry.agent_view(self.agent)

        reply = await self.scheduler.run_step(
            command=self._workflow_command("schedule", trigger),
            context=context,
        )
        if reply is not None:
            scheduler_decision = {
                "decision": reply.decision,
                "reason": reply.reason,
            }

        context = self.engine.registry.agent_view(self.agent)
        topic_snapshot = context.topic_snapshot
        await self._emit(
            "debug.event.agent.workflow.completed",
            trigger=trigger,
            effective_watermark=max(trigger.trigger_watermark, self.engine.registry.watermark),
            rerun_pending=bool(self._pending),
            generated_candidate=generated_candidate,
            scheduler_decision=scheduler_decision,
        )
        if self.config.topic.enabled and (topic_snapshot is None or topic_snapshot.watermark < context.watermark):
            self._ensure_background_analysis(trigger, context)
        self._maybe_schedule_follow_up(trigger, reply)

    async def _run_mafia_spinup(
        self,
        trigger: WorkflowTrigger,
        *,
        reason: str,
        extra_payload: dict[str, Any],
    ) -> None:
        generated_candidate: dict[str, object] | None = None
        context = self.engine.registry.agent_view(self.agent)
        await self.buffer_worker.discard_stale(command=self._workflow_command("evict", trigger))
        context = self.engine.registry.agent_view(self.agent)
        buffer_before = self.engine.registry.buffer_for(self.agent.id)
        should_generate = not buffer_before
        if extra_payload.get("reactive"):
            newest_source = max((candidate.source_watermark for candidate in buffer_before), default=-1)
            should_generate = should_generate or newest_source < context.watermark
        if should_generate:
            candidate = await self.generator.run_step(
                command=self._workflow_command("generate", trigger, extra_payload=extra_payload),
                context=context,
                enqueue_schedule=False,
            )
            if candidate is not None:
                generated_candidate = {
                    "candidate_id": candidate.candidate_id,
                    "text": candidate.text,
                    "source_watermark": candidate.source_watermark,
                }
        await self._emit(
            "debug.event.agent.workflow.completed",
            trigger=trigger,
            effective_watermark=max(trigger.trigger_watermark, self.engine.registry.watermark),
            rerun_pending=bool(self._pending),
            generated_candidate=generated_candidate,
            scheduler_decision={
                "decision": "wait",
                "reason": reason,
            },
        )

    def _workflow_command(
        self,
        step: str,
        trigger: WorkflowTrigger,
        extra_payload: dict[str, Any] | None = None,
    ) -> CommandEnvelope:
        payload = {
            "workflow": True,
            "trigger_kind": trigger.trigger_kind,
            "trigger_watermark": trigger.trigger_watermark,
            "trigger_message_id": trigger.trigger_message_id,
        }
        if extra_payload:
            payload.update(extra_payload)
        return CommandEnvelope(
            subject=f"agent.workflow.{self.agent.id}.{step}",
            correlation_id=self.engine.run_id,
            payload=payload,
        )

    async def _emit(
        self,
        subject: str,
        *,
        trigger: WorkflowTrigger,
        effective_watermark: int,
        rerun_pending: bool,
        reason: str | None = None,
        generated_candidate: dict[str, object] | None = None,
        scheduler_decision: dict[str, object] | None = None,
    ) -> None:
        payload: dict[str, Any] = {
            "agent_id": self.agent.id,
            "display_name": self.agent.display_name,
            "trigger_kind": trigger.trigger_kind,
            "trigger_message_id": trigger.trigger_message_id,
            "trigger_watermark": trigger.trigger_watermark,
            "effective_watermark": effective_watermark,
            "rerun_pending": rerun_pending,
            "generated_candidate": generated_candidate,
            "scheduler_decision": scheduler_decision,
        }
        if reason is not None:
            payload["reason"] = reason
        await self.engine.append_event(make_event(subject, payload=payload))

    def _maybe_schedule_follow_up(
        self,
        trigger: WorkflowTrigger,
        reply: SchedulerReply | None,
    ) -> None:
        if self._closed or self._pending is not None:
            return
        if self.engine.registry.run_state() != "running":
            return
        if self.engine.registry.active_reservation_for(self.agent.id) is not None:
            return
        if not self.engine.registry.buffer_for(self.agent.id):
            return
        if reply is None or reply.decision != "wait":
            return
        if reply.reason not in {
            "conversation-active",
            "duplicate-recent-message",
            "duplicate-inflight-message",
            "room-idle-wait",
            "recent-own-message",
        }:
            return
        delay = self._follow_up_delay(reply.reason)
        self._follow_up_task = self.engine.create_background_task(
            self._delayed_follow_up(trigger, delay),
            f"workflow-follow-up.{self.agent.id}",
        )

    async def _delayed_follow_up(self, trigger: WorkflowTrigger, delay: float) -> None:
        try:
            await asyncio.sleep(delay)
            if self._closed or self.engine.registry.run_state() != "running":
                return
            await self.notify(
                WorkflowTrigger(
                    trigger_kind="follow_up",
                    trigger_watermark=self.engine.registry.watermark,
                    trigger_message_id=trigger.trigger_message_id,
                    trigger_sender_id=trigger.trigger_sender_id,
                )
            )
        finally:
            self._follow_up_task = None

    def _cancel_follow_up(self) -> None:
        if self._follow_up_task and not self._follow_up_task.done():
            self._follow_up_task.cancel()
        self._follow_up_task = None

    def _ensure_background_analysis(self, trigger: WorkflowTrigger, context: AgentContextSnapshot) -> None:
        if self._closed:
            return
        if self._analysis_task and not self._analysis_task.done():
            return
        self._analysis_task = self.engine.create_background_task(
            self._run_background_analysis(trigger, context),
            f"workflow-analyze.{self.agent.id}",
        )

    async def _run_background_analysis(self, trigger: WorkflowTrigger, context: AgentContextSnapshot) -> None:
        try:
            await self.analyzer.run_step(
                command=self._workflow_command("analyze", trigger),
                context=context,
            )
        finally:
            self._analysis_task = None

    def _follow_up_delay(self, reason: str) -> float:
        reactivity = self.agent.personality.reactivity
        talkativeness = self.agent.personality.talkativeness
        if self.config.room_mode.value == "mafia":
            reactivity += 0.35
            talkativeness += 0.1
            base = max(0.03, 0.28 - (reactivity * 0.2) - (talkativeness * 0.1))
        else:
            base = max(0.08, 0.42 - (reactivity * 0.2) - (talkativeness * 0.12))
        if reason == "recent-own-message":
            return min(0.55 if self.config.room_mode.value == "mafia" else 0.8, base + (0.14 if self.config.room_mode.value == "mafia" else 0.22))
        if reason.startswith("duplicate-"):
            return min(0.45 if self.config.room_mode.value == "mafia" else 0.65, base + (0.08 if self.config.room_mode.value == "mafia" else 0.16))
        if reason == "conversation-active":
            return min(0.18 if self.config.room_mode.value == "mafia" else 0.35, base)
        return min(0.24 if self.config.room_mode.value == "mafia" else 0.5, base)


class ConversationEngineProtocol:
    registry: object
    policies: PolicySet
    clock: object
    run_id: str

    async def append_event(self, event): ...
    async def append_event_and_wait(self, event): ...
    async def dispatch_command(self, command: CommandEnvelope) -> None: ...
    async def enqueue_command(self, command: CommandEnvelope) -> None: ...
    async def note_worker_failure(self, agent_id: str, worker_kind: str, error: Exception) -> None: ...
    async def note_worker_success(self, agent_id: str, worker_kind: str) -> None: ...
    def create_background_task(self, coro, name: str) -> asyncio.Task[Any]: ...
