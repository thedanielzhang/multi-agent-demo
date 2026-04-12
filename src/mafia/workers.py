from __future__ import annotations

import asyncio
import time
from datetime import timedelta
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
    DeliveryReservation,
    GeneratorInputSnapshot,
    GeneratorReply,
    SchedulerInputSnapshot,
    SchedulerReply,
    TopicShift,
    TopicWeight,
    TransportMessagePayload,
    make_event,
    utc_now,
)
from mafia.policies import PolicySet


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
            "reactive": bool(payload.get("reactive", False)),
            "heartbeat": bool(payload.get("heartbeat", False)),
            "bootstrap": bool(payload.get("bootstrap", False)),
            "follow_up": int(payload.get("follow_up", 0) or 0),
            "message_id": payload.get("message_id"),
        }

    def _prompt_preview(self, prompt: str, limit: int = 500) -> str:
        compact = " ".join(prompt.split())
        if len(compact) <= limit:
            return compact
        return compact[: limit - 3] + "..."

    def _input_summary(self, input_data) -> dict[str, object]:
        if isinstance(input_data, SchedulerInputSnapshot):
            context = input_data.agent_context
            return {
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
        if isinstance(input_data, GeneratorInputSnapshot):
            context = input_data.agent_context
            return {
                "scenario": input_data.scenario,
                "watermark": context.watermark,
                "focus_count": len(context.focus_messages),
                "focus_preview": [message.text for message in context.focus_messages[-3:]],
                "room_keywords": list(context.room_metrics.recent_keyword_sketch),
                "topic_snapshot_id": context.topic_snapshot_id,
                "max_words": input_data.max_words,
                "style_prompt": input_data.style_prompt,
            }
        if isinstance(input_data, AnalyzerInputSnapshot):
            return {
                "scenario": input_data.scenario,
                "message_count": len(input_data.recent_messages),
                "message_preview": [message.text for message in input_data.recent_messages[-4:]],
                "seed_topics": list(input_data.seed_topics),
                "previous_snapshot_id": input_data.previous_snapshot.snapshot_id if input_data.previous_snapshot else None,
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
        return {"type": type(result).__name__}


class AgentTopicAnalyzerWorker(BaseAgentWorker):
    async def handle_tick(self, command: CommandEnvelope) -> None:
        if self.config.mode == ModeProfile.BASELINE_TIME_TO_TALK or not self.config.topic.enabled:
            return
        context = self.engine.registry.agent_view(self.agent)
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
        await self.engine.append_event(
            make_event(
                f"topic.event.{self.agent.id}.snapshot.updated",
                command=command,
                payload=snapshot,
            )
        )


class AgentGenerationWorker(BaseAgentWorker):
    async def handle_tick(self, command: CommandEnvelope) -> None:
        context = self.engine.registry.agent_view(self.agent)
        reactive_refresh = bool(command.payload.get("reactive"))
        if not self.policies.should_generate(context, self.agent.generation.buffer_size):
            if not reactive_refresh or self.config.mode != ModeProfile.IMPROVED_BUFFERED_ASYNC:
                return
            buffer = self.engine.registry.buffer_for(self.agent.id)
            if not buffer:
                return
            newest_source = max(candidate.source_watermark for candidate in buffer)
            if newest_source >= context.watermark:
                return
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
        generator_input = self.policies.generator_input(self.agent, context)
        reply = await self._invoke(
            self.actors.generator,
            prompt=self.policies.prompts.generator_prompt(self.agent, generator_input),
            input_data=generator_input,
            output_type=GeneratorReply,
            command=command,
        )
        if reply is None:
            return
        candidate = self._candidate_from_context(context, reply.text)
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


class AgentBufferWorker(BaseAgentWorker):
    async def handle_evict_tick(self, command: CommandEnvelope) -> None:
        await self._discard_stale(command)

    async def _discard_stale(self, command: CommandEnvelope) -> None:
        if self.config.mode != ModeProfile.IMPROVED_BUFFERED_ASYNC:
            return
        now = utc_now()
        for candidate in self.engine.registry.buffer_for(self.agent.id):
            if not self.policies.candidate_is_stale(self.agent, candidate, now):
                continue
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


class AgentSchedulerWorker(BaseAgentWorker):
    async def handle_tick(self, command: CommandEnvelope) -> None:
        if self.config.mode == ModeProfile.IMPROVED_BUFFERED_ASYNC:
            await AgentBufferWorker(self.engine, self.config, self.agent, self.actors, self.invoker, self.policies)._discard_stale(command)
        context = self.engine.registry.agent_view(self.agent)
        buffer_candidates = self.engine.registry.buffer_for(self.agent.id)
        active_reservations = self.engine.registry.active_reservations()
        scheduler_input = self.policies.scheduler_input(
            self.agent,
            context,
            buffer_candidates=buffer_candidates,
            active_reservations=active_reservations,
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
            return
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
            return
        if self.config.mode == ModeProfile.BASELINE_TIME_TO_TALK:
            await self._send_baseline_candidate(command, context)
        else:
            await self._send_buffered_candidate(command, context)

    async def _send_baseline_candidate(self, command: CommandEnvelope, context) -> None:
        generator_input = self.policies.generator_input(self.agent, context)
        reply = await self._invoke(
            self.actors.generator,
            prompt=self.policies.prompts.generator_prompt(self.agent, generator_input),
            input_data=generator_input,
            output_type=GeneratorReply,
            command=command,
        )
        if reply is None:
            return
        candidate = self._candidate_from_context(context, reply.text)
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
        candidates = self.engine.registry.buffer_for(self.agent.id)
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
        await self.engine.clock.schedule_delivery(
            delay,
            CommandEnvelope(
                subject=f"agent.command.{self.agent.id}.deliver.submit",
                correlation_id=command.correlation_id,
                causation_id=command.command_id,
                payload={
                    "agent_id": self.agent.id,
                    "reservation_id": reservation.reservation_id,
                },
            ),
        )

    async def handle_submit(self, command: CommandEnvelope) -> None:
        reservation = self.engine.registry.reservation_for(command.payload["reservation_id"])
        if reservation is None:
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


class ConversationEngineProtocol:
    registry: object
    policies: PolicySet
    clock: object

    async def append_event(self, event): ...
    async def append_event_and_wait(self, event): ...
    async def dispatch_command(self, command: CommandEnvelope) -> None: ...
    async def enqueue_command(self, command: CommandEnvelope) -> None: ...
    async def note_worker_failure(self, agent_id: str, worker_kind: str, error: Exception) -> None: ...
    async def note_worker_success(self, agent_id: str, worker_kind: str) -> None: ...
