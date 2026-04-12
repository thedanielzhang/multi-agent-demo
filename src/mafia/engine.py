from __future__ import annotations

import asyncio
import contextlib
import json
import time
from collections import defaultdict
from pathlib import Path
from typing import Any
from uuid import uuid4

from mafia.agent import AgentInvoker, build_agent_actors
from mafia.bus import SubjectBus
from mafia.command_router import CommandRouter
from mafia.compose_compat import AgentRuntime
from mafia.config import AgentConfig, AppConfig, ModeProfile
from mafia.event_log import EventLog
from mafia.messages import (
    CommandEnvelope,
    ConversationMessage,
    EventEnvelope,
    LoggedEvent,
    MessagePayload,
    SenderKind,
    make_event,
    utc_now,
)
from mafia.policies import PolicySet
from mafia.projections import ProjectionRegistry
from mafia.runtimes import build_runtime
from mafia.runtime_support import InMemorySessionStore, build_workspace
from mafia.transport import build_transport
from mafia.workers import (
    AgentBufferWorker,
    AgentDeliveryWorker,
    AgentGenerationWorker,
    AgentSchedulerWorker,
    AgentTopicAnalyzerWorker,
)


class EventDispatcher:
    def __init__(self, event_log: EventLog, registry: ProjectionRegistry, bus: SubjectBus) -> None:
        self._event_log = event_log
        self._registry = registry
        self._bus = bus
        self._task: asyncio.Task[None] | None = None

    @property
    def task(self) -> asyncio.Task[None] | None:
        return self._task

    async def start(self) -> None:
        self._task = asyncio.create_task(self._run())

    async def close(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def _run(self) -> None:
        seq = 0
        while True:
            events = await self._event_log.wait_for_events(seq)
            for logged_event in events:
                await self._registry.wait_until(logged_event.seq)
                await self._bus.publish(logged_event.event.subject, logged_event.event)
                seq = logged_event.seq


class ClockService:
    def __init__(self, config: AppConfig, command_router: CommandRouter, registry: ProjectionRegistry) -> None:
        self._config = config
        self._command_router = command_router
        self._registry = registry
        self._tasks: list[asyncio.Task[None]] = []
        self._one_shots: set[asyncio.Task[None]] = set()
        self._running_gate = asyncio.Event()
        self._stop_event = asyncio.Event()
        self._started_at: float | None = None
        self._paused_started_at: float | None = None
        self._paused_total = 0.0
        self._monitor_task: callable | None = None

    async def start(self, monitor_task) -> None:
        self._monitor_task = monitor_task
        self._spawn("clock.housekeeping", self._housekeeping())
        for index, agent in enumerate(self._config.agents):
            base_stagger = 0.12 * index
            if self._config.mode == ModeProfile.BASELINE_TIME_TO_TALK:
                self._spawn(
                    f"clock.schedule.{agent.id}",
                    self._periodic(
                        f"agent.command.{agent.id}.schedule.tick",
                        agent.scheduler.tick_rate_seconds,
                        initial_delay=base_stagger,
                    ),
                )
            if self._config.mode == ModeProfile.IMPROVED_BUFFERED_ASYNC:
                self._spawn(
                    f"clock.schedule.{agent.id}",
                    self._jittered_periodic(
                        f"agent.command.{agent.id}.schedule.tick",
                        agent.scheduler.tick_rate_seconds,
                        initial_delay=base_stagger + 0.22,
                        phase=index,
                        payload={"heartbeat": True},
                    ),
                )
                self._spawn(
                    f"clock.generate.{agent.id}",
                    self._periodic(
                        f"agent.command.{agent.id}.generate.tick",
                        agent.generation.tick_rate_seconds,
                        initial_delay=base_stagger + 0.05,
                    ),
                )
                self._spawn(
                    f"clock.topic.{agent.id}",
                    self._periodic(
                        f"topic.command.{agent.id}.analyze.tick",
                        self._config.topic.tick_rate_seconds,
                        initial_delay=base_stagger + 0.08,
                    ),
                )
                self._spawn(
                    f"clock.evict.{agent.id}",
                    self._periodic(
                        f"agent.command.{agent.id}.buffer.evict.tick",
                        min(agent.generation.tick_rate_seconds, 0.5),
                        initial_delay=base_stagger + 0.03,
                    ),
                )

    async def close(self) -> None:
        self._stop_event.set()
        for task in self._tasks:
            task.cancel()
        for task in self._one_shots:
            task.cancel()
        await asyncio.gather(*self._tasks, *self._one_shots, return_exceptions=True)

    def set_state(self, state: str) -> None:
        if state == "running":
            if self._started_at is None:
                self._started_at = time.monotonic()
            if self._paused_started_at is not None:
                self._paused_total += time.monotonic() - self._paused_started_at
                self._paused_started_at = None
            self._running_gate.set()
        elif state == "paused":
            if self._paused_started_at is None:
                self._paused_started_at = time.monotonic()
            self._running_gate.clear()
        elif state in {"stopping", "stopped", "failed"}:
            self._running_gate.clear()
            self._stop_event.set()

    async def schedule_delivery(self, delay_seconds: float, command: CommandEnvelope) -> None:
        await self.schedule_command(delay_seconds, command)

    async def schedule_command(self, delay_seconds: float, command: CommandEnvelope) -> None:
        task = asyncio.create_task(self._one_shot(delay_seconds, command))
        self._one_shots.add(task)
        task.add_done_callback(self._one_shots.discard)
        if self._monitor_task:
            self._monitor_task(task, f"clock.oneshot.{command.subject}")

    def _spawn(self, name: str, coro) -> None:
        task = asyncio.create_task(coro)
        self._tasks.append(task)
        if self._monitor_task:
            self._monitor_task(task, name)

    async def _periodic(self, subject: str, interval: float, initial_delay: float = 0.0) -> None:
        if initial_delay > 0:
            await self._sleep_resumable(initial_delay)
        while not self._stop_event.is_set():
            await self._sleep_resumable(interval)
            if self._stop_event.is_set():
                return
            if self._registry.run_state() != "running":
                continue
            await self._command_router.enqueue(CommandEnvelope(subject=subject))

    async def _jittered_periodic(
        self,
        subject: str,
        interval: float,
        *,
        initial_delay: float = 0.0,
        phase: int = 0,
        payload: dict[str, object] | None = None,
    ) -> None:
        beat = 0
        if initial_delay > 0:
            await self._sleep_resumable(initial_delay)
        while not self._stop_event.is_set():
            factor = self._heartbeat_factor(phase, beat)
            await self._sleep_resumable(max(0.05, interval * factor))
            if self._stop_event.is_set():
                return
            if self._registry.run_state() != "running":
                continue
            await self._command_router.enqueue(
                CommandEnvelope(subject=subject, payload=dict(payload or {}))
            )
            beat += 1

    def _heartbeat_factor(self, phase: int, beat: int) -> float:
        factors = (0.82, 1.07, 0.91, 1.16, 0.97, 1.11)
        return factors[(phase + beat) % len(factors)]

    async def _one_shot(self, delay_seconds: float, command: CommandEnvelope) -> None:
        await self._sleep_resumable(delay_seconds)
        if self._stop_event.is_set():
            return
        while self._registry.run_state() == "paused" and not self._stop_event.is_set():
            await self._running_gate.wait()
        if self._stop_event.is_set() or self._registry.run_state() != "running":
            return
        await self._command_router.enqueue(command)

    async def _sleep_resumable(self, delay_seconds: float) -> None:
        remaining = delay_seconds
        while remaining > 0 and not self._stop_event.is_set():
            await self._running_gate.wait()
            slice_seconds = min(remaining, 0.05)
            started = time.monotonic()
            await asyncio.sleep(slice_seconds)
            if self._running_gate.is_set():
                remaining -= time.monotonic() - started

    async def _housekeeping(self) -> None:
        while not self._stop_event.is_set():
            await asyncio.sleep(0.1)
            if self._registry.run_state() != "running" or self._started_at is None:
                continue
            elapsed = time.monotonic() - self._started_at - self._paused_total
            if (
                self._config.chat.max_duration_seconds is not None
                and elapsed >= self._config.chat.max_duration_seconds
            ):
                await self._command_router.enqueue(CommandEnvelope(subject="run.command.stop"))
                continue
            if (
                self._config.chat.max_messages is not None
                and len(self._registry.latest_messages()) >= self._config.chat.max_messages
            ):
                await self._command_router.enqueue(CommandEnvelope(subject="run.command.stop"))


class ConversationEngine:
    def __init__(self, config: AppConfig, runtime: AgentRuntime | None = None) -> None:
        self.config = config
        self.run_id = str(uuid4())
        self.workspace = build_workspace(self.run_id)
        self.session_store = InMemorySessionStore()
        self.runtime = runtime or build_runtime(
            config.runtime,
            session_store=self.session_store,
            on_message=None,
            interactive_roles=set(),
        )
        self.event_log = EventLog()
        self.bus = SubjectBus()
        self.command_router = CommandRouter()
        self.registry = ProjectionRegistry(self.event_log, config)
        self.dispatcher = EventDispatcher(self.event_log, self.registry, self.bus)
        self.clock = ClockService(config, self.command_router, self.registry)
        self.policies = PolicySet(config)
        self.invoker = AgentInvoker(self.runtime, self.run_id, self.workspace)
        self._tasks_started = False
        self._closed = False
        self._stopped = asyncio.Event()
        self._worker_failures: dict[str, dict[str, int]] = defaultdict(
            lambda: {"total": 0, "consecutive": 0}
        )
        self._agent_actors = {agent.id: build_agent_actors(agent) for agent in config.agents}
        self._critical_tasks: dict[asyncio.Task[Any], str] = {}
        self._handling_failure = False
        self._transport = build_transport(self, config.transport)
        self._reactive_mailboxes: dict[str, asyncio.Queue[ConversationMessage]] = {
            agent.id: asyncio.Queue(maxsize=32) for agent in config.agents
        }
        self._reactive_tasks: dict[str, asyncio.Task[None]] = {}
        self._bootstrap_tasks: list[asyncio.Task[None]] = []
        self._register_handlers()
        self._register_reactive_subscriptions()

    async def start(self) -> None:
        if self._tasks_started:
            return
        self._tasks_started = True
        await self.registry.start()
        if self.registry.task:
            self._monitor_task(self.registry.task, "registry")
        await self.command_router.start(self._handle_command_failure)
        for idx, task in enumerate(self.command_router.worker_tasks):
            self._monitor_task(task, f"command-router.{idx}")
        await self.dispatcher.start()
        if self.dispatcher.task:
            self._monitor_task(self.dispatcher.task, "dispatcher")
        await self.clock.start(self._monitor_task)
        await self._start_reactive_reactors()
        await self.dispatch_command(CommandEnvelope(subject="run.command.start"))
        await self._wait_for_state("running")
        if self.config.mode == ModeProfile.IMPROVED_BUFFERED_ASYNC:
            await self._bootstrap_improved_agents()

    async def run(self) -> None:
        await self.start()
        await self._stopped.wait()
        await self.close()

    async def close(self) -> None:
        if self._closed:
            return
        if self._tasks_started and self.registry.run_state() in {"starting", "running", "paused"}:
            with contextlib.suppress(Exception):
                await self.dispatch_command(CommandEnvelope(subject="run.command.stop"))
            with contextlib.suppress(Exception):
                await self._wait_for_state("stopped", timeout=2.0)
        self._closed = True
        for task in self._reactive_tasks.values():
            task.cancel()
        for task in self._bootstrap_tasks:
            task.cancel()
        await asyncio.gather(*self._bootstrap_tasks, return_exceptions=True)
        await asyncio.gather(*self._reactive_tasks.values(), return_exceptions=True)
        await self.clock.close()
        await self.command_router.close()
        await self.dispatcher.close()
        await self.registry.close()
        await self.bus.close()
        self._stopped.set()

    async def dispatch_command(self, command: CommandEnvelope) -> None:
        await self.command_router.dispatch(command)

    async def enqueue_command(self, command: CommandEnvelope) -> None:
        await self.command_router.enqueue(command)

    async def publish_command(self, command: CommandEnvelope) -> None:
        await self.dispatch_command(command)

    async def submit_message(
        self,
        *,
        text: str,
        sender_id: str = "human",
        display_name: str = "Human",
        sender_kind: SenderKind = SenderKind.HUMAN,
        mentions: list[str] | None = None,
        client_message_id: str | None = None,
        reply_hint: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        await self.dispatch_command(
            CommandEnvelope(
                subject="conversation.command.message.submit",
                payload=MessagePayload(
                    client_message_id=client_message_id or str(uuid4()),
                    sender_id=sender_id,
                    sender_kind=sender_kind,
                    display_name=display_name,
                    text=text,
                    mentions=mentions or [],
                    reply_hint=reply_hint,
                    metadata=metadata or {},
                ).model_dump(mode="json"),
            )
        )

    async def export_events(self) -> list[LoggedEvent]:
        return await self.event_log.snapshot()

    async def note_worker_failure(self, agent_id: str, worker_kind: str, error: Exception) -> None:
        key = f"{agent_id}:{worker_kind}"
        stats = self._worker_failures[key]
        stats["total"] += 1
        stats["consecutive"] += 1
        await self.append_event(
            EventEnvelope(
                subject="debug.event.worker.failed",
                correlation_id=self.run_id,
                payload={
                    "agent_id": agent_id,
                    "worker_kind": worker_kind,
                    "error": str(error),
                    "failures": stats["total"],
                    "consecutive_failures": stats["consecutive"],
                },
            )
        )
        if stats["consecutive"] >= 3:
            await self._force_fail_run("worker_failure", error=str(error))

    async def note_worker_success(self, agent_id: str, worker_kind: str) -> None:
        key = f"{agent_id}:{worker_kind}"
        stats = self._worker_failures.get(key)
        if not stats:
            return
        stats["consecutive"] = 0

    async def append_event(self, event: EventEnvelope) -> LoggedEvent:
        return await self.event_log.append(event)

    async def append_event_and_wait(self, event: EventEnvelope) -> LoggedEvent:
        logged = await self.append_event(event)
        await self.registry.wait_until(logged.seq)
        return logged

    async def _wait_for_state(self, state: str, timeout: float = 1.0) -> None:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.registry.run_state() == state:
                return
            await asyncio.sleep(0.01)
        raise TimeoutError(f"run state did not reach {state!r}")

    def _monitor_task(self, task: asyncio.Task[Any], name: str) -> None:
        self._critical_tasks[task] = name
        task.add_done_callback(self._task_done)

    def _task_done(self, task: asyncio.Task[Any]) -> None:
        name = self._critical_tasks.pop(task, "unknown")
        if task.cancelled():
            return
        exc = task.exception()
        if exc is None:
            return
        asyncio.create_task(self._handle_infrastructure_failure(name, exc))

    async def _handle_command_failure(self, subject: str, exc: Exception) -> None:
        await self._handle_infrastructure_failure(f"command:{subject}", exc)

    async def _handle_infrastructure_failure(self, name: str, exc: Exception) -> None:
        if self._handling_failure or self.registry.run_state() in {"failed", "stopped"}:
            return
        self._handling_failure = True
        with contextlib.suppress(Exception):
            await self.append_event(
                EventEnvelope(
                    subject="debug.event.infrastructure.failed",
                    correlation_id=self.run_id,
                    payload={"component": name, "error": str(exc)},
                )
            )
        await self._force_fail_run("infrastructure_failure", component=name, error=str(exc))

    async def _force_fail_run(self, reason: str, **payload: Any) -> None:
        with contextlib.suppress(Exception):
            await self._abort_active_reservations(reason)
        with contextlib.suppress(Exception):
            logged = await self.event_log.append(
                EventEnvelope(
                    subject="run.event.state.changed",
                    correlation_id=self.run_id,
                    payload={"state": "failed", "reason": reason, **payload},
                )
            )
            if self.registry.task and not self.registry.task.done():
                await asyncio.wait_for(self.registry.wait_until(logged.seq), timeout=0.25)
        self.clock.set_state("failed")
        self._stopped.set()

    def _register_handlers(self) -> None:
        self.command_router.register("run.command.start", self._handle_start)
        self.command_router.register("run.command.pause", self._handle_pause)
        self.command_router.register("run.command.resume", self._handle_resume)
        self.command_router.register("run.command.stop", self._handle_stop)
        self.command_router.register("run.command.export", self._handle_export)
        self.command_router.register("conversation.command.message.submit", self._handle_submit_message)
        self.command_router.register("transport.command.message.send", self._transport.handle_send)

        for agent in self.config.agents:
            actors = self._agent_actors[agent.id]
            analyzer = AgentTopicAnalyzerWorker(self, self.config, agent, actors, self.invoker, self.policies)
            generator = AgentGenerationWorker(self, self.config, agent, actors, self.invoker, self.policies)
            buffer_worker = AgentBufferWorker(self, self.config, agent, actors, self.invoker, self.policies)
            scheduler = AgentSchedulerWorker(self, self.config, agent, actors, self.invoker, self.policies)
            delivery = AgentDeliveryWorker(self, self.config, agent)
            self.command_router.register(f"topic.command.{agent.id}.analyze.tick", analyzer.handle_tick)
            self.command_router.register(f"agent.command.{agent.id}.generate.tick", generator.handle_tick)
            self.command_router.register(f"agent.command.{agent.id}.buffer.evict.tick", buffer_worker.handle_evict_tick)
            self.command_router.register(f"agent.command.{agent.id}.schedule.tick", scheduler.handle_tick)
            self.command_router.register(f"agent.command.{agent.id}.deliver.request", delivery.handle_request)
            self.command_router.register(f"agent.command.{agent.id}.deliver.submit", delivery.handle_submit)
            self.command_router.register(f"agent.command.{agent.id}.delivery.transport_acked", delivery.handle_transport_acked)
            self.command_router.register(f"agent.command.{agent.id}.delivery.transport_failed", delivery.handle_transport_failed)

    async def _set_state(self, state: str, command: CommandEnvelope | None = None, **payload: Any) -> None:
        logged = await self.append_event(
            make_event(
                "run.event.state.changed",
                command=command,
                payload={"state": state, **payload},
            )
        )
        await self.registry.wait_until(logged.seq)
        self.clock.set_state(state)
        if state in {"stopped", "failed"}:
            self._stopped.set()

    async def _handle_start(self, command: CommandEnvelope) -> None:
        await self.append_event(
            make_event(
                "run.event.started",
                command=command,
                payload={
                    "run_id": self.run_id,
                    "mode": self.config.mode,
                    "policy_profile": self.config.policy_profile.model_dump(mode="json"),
                    "runtime_provider": self.config.runtime.provider,
                    "runtime_model": self.config.runtime.model,
                    "transport_provider": self.config.transport.provider,
                    "scenario": self.config.chat.scenario,
                    "max_duration_seconds": self.config.chat.max_duration_seconds,
                    "max_messages": self.config.chat.max_messages,
                },
            )
        )
        await self._set_state("starting", command)
        await self._set_state("running", command)

    async def _handle_pause(self, command: CommandEnvelope) -> None:
        if self.registry.run_state() == "running":
            await self._set_state("paused", command)

    async def _handle_resume(self, command: CommandEnvelope) -> None:
        if self.registry.run_state() == "paused":
            await self._set_state("running", command)

    async def _handle_stop(self, command: CommandEnvelope) -> None:
        if self.registry.run_state() in {"stopped", "failed"}:
            return
        await self._abort_active_reservations("run_stopping", command=command)
        await self._set_state("stopping", command)
        await self._set_state("stopped", command)

    async def _abort_active_reservations(self, reason: str, command: CommandEnvelope | None = None) -> None:
        for reservation in self.registry.active_reservations():
            await self.append_event(
                make_event(
                    f"agent.event.{reservation.agent_id}.delivery.aborted",
                    command=command,
                    payload={
                        "agent_id": reservation.agent_id,
                        "reservation_id": reservation.reservation_id,
                        "candidate_id": reservation.candidate.candidate_id,
                        "reason": reason,
                    },
                )
            )

    async def _handle_export(self, command: CommandEnvelope) -> None:
        await self.append_event(
            make_event(
                "debug.event.export.requested",
                command=command,
                payload={"run_id": self.run_id},
            )
        )

    async def _handle_submit_message(self, command: CommandEnvelope) -> None:
        payload = MessagePayload.model_validate(command.payload)
        if self.registry.has_client_message_id(payload.client_message_id):
            return
        message = ConversationMessage(
            message_id=str(uuid4()),
            client_message_id=payload.client_message_id,
            sender_id=payload.sender_id,
            sender_kind=payload.sender_kind,
            display_name=payload.display_name,
            text=payload.text,
            created_at=utc_now(),
            sequence_no=len(self.registry.latest_messages()) + 1,
            mentions=payload.mentions,
            reply_hint=payload.reply_hint,
            metadata=payload.metadata,
        )
        logged = await self.append_event(
            make_event(
                "conversation.event.message.committed",
                command=command,
                payload=message,
            )
        )
        await self.registry.wait_until(logged.seq)

    def _register_reactive_subscriptions(self) -> None:
        if self.config.mode != ModeProfile.IMPROVED_BUFFERED_ASYNC:
            return
        for index, agent in enumerate(self.config.agents):
            self.bus.subscribe(
                "conversation.event.message.committed",
                self._make_agent_reactive_handler(agent, index=index),
                maxsize=128,
                overflow="drop_oldest",
            )

    def _make_agent_reactive_handler(self, agent: AgentConfig, *, index: int):
        async def _handle(_subject: str, event: EventEnvelope) -> None:
            await self._handle_committed_message_reactive_nudge_for_agent(agent, index=index, event=event)

        return _handle

    async def _start_reactive_reactors(self) -> None:
        if self.config.mode != ModeProfile.IMPROVED_BUFFERED_ASYNC:
            return
        for index, agent in enumerate(self.config.agents):
            if agent.id in self._reactive_tasks:
                continue
            task = asyncio.create_task(self._reactive_agent_loop(agent, index=index))
            self._reactive_tasks[agent.id] = task
            self._monitor_task(task, f"reactive-agent.{agent.id}")

    async def _handle_committed_message_reactive_nudge_for_agent(
        self,
        agent: AgentConfig,
        *,
        index: int,
        event: EventEnvelope,
    ) -> None:
        message = ConversationMessage.model_validate(event.payload)
        queue = self._reactive_mailboxes[agent.id]
        if queue.full():
            with contextlib.suppress(asyncio.QueueEmpty):
                queue.get_nowait()
                queue.task_done()
        await queue.put(message)

    async def _bootstrap_improved_agents(self) -> None:
        for index, agent in enumerate(self.config.agents):
            task = asyncio.create_task(self._bootstrap_agent(agent, index=index))
            self._bootstrap_tasks.append(task)
            self._monitor_task(task, f"bootstrap.{agent.id}")

    async def _bootstrap_agent(self, agent: AgentConfig, *, index: int) -> None:
        await asyncio.sleep(0.05 + (0.12 * index))
        if self.registry.run_state() != "running":
            return
        previous_buffer_version = self.registry.buffer_version_for(agent.id)
        await self.dispatch_command(
            CommandEnvelope(
                subject=f"agent.command.{agent.id}.generate.tick",
                payload={"bootstrap": True},
            )
        )
        await self._wait_for_buffer_update(agent.id, previous_version=previous_buffer_version, timeout=0.5)
        if self.registry.run_state() != "running":
            return
        if self.registry.active_reservation_for(agent.id) is not None:
            return
        if not self.registry.buffer_for(agent.id):
            return
        await self.dispatch_command(
            CommandEnvelope(
                subject=f"agent.command.{agent.id}.schedule.tick",
                payload={"bootstrap": True},
            )
        )

    def _reactive_schedule_delay(self, agent: AgentConfig, *, index: int) -> float:
        base = min(
            agent.scheduler.tick_rate_seconds * 0.6,
            0.06 + ((1.0 - agent.personality.reactivity) * 0.45),
        )
        persona_offset = (index % 5) * 0.015
        return min(agent.scheduler.tick_rate_seconds, max(0.04, base + persona_offset))

    def _reactive_follow_up_delay(self, agent: AgentConfig, *, index: int, attempt: int) -> float:
        base = max(
            0.16,
            min(agent.scheduler.tick_rate_seconds * 4.0, self._reactive_schedule_delay(agent, index=index) * 1.2),
        )
        return min(agent.scheduler.tick_rate_seconds * 6.0, base + (attempt * 0.14))

    def _reactive_follow_up_attempts(self, agent: AgentConfig) -> int:
        return max(1, min(3, 1 + round(agent.personality.reactivity * 2)))

    async def _reactive_agent_loop(self, agent: AgentConfig, *, index: int) -> None:
        queue = self._reactive_mailboxes[agent.id]
        while True:
            pending = await queue.get()
            queue.task_done()
            pending = await self._coalesce_reactive_messages(agent, index=index, queue=queue, pending=pending)
            if pending.sender_id == agent.id:
                continue
            while True:
                if self.registry.run_state() != "running":
                    break
                if self.registry.active_reservation_for(agent.id) is not None:
                    newer = await self._wait_for_reactive_message(queue, timeout=0.05)
                    if newer is None:
                        continue
                    if newer.sender_id == agent.id:
                        continue
                    pending = await self._coalesce_reactive_messages(
                        agent,
                        index=index,
                        queue=queue,
                        pending=newer,
                    )
                    continue
                should_retry = await self._run_reactive_attempt(agent, pending)
                if not should_retry:
                    break
                restarted = False
                for attempt in range(self._reactive_follow_up_attempts(agent)):
                    newer = await self._wait_for_reactive_message(
                        queue,
                        timeout=self._reactive_follow_up_delay(agent, index=index, attempt=attempt),
                    )
                    if newer is not None:
                        pending = await self._coalesce_reactive_messages(
                            agent,
                            index=index,
                            queue=queue,
                            pending=newer,
                        )
                        restarted = True
                        break
                    if self.registry.run_state() != "running":
                        break
                    if self.registry.active_reservation_for(agent.id) is not None:
                        break
                    should_retry = await self._run_reactive_attempt(
                        agent,
                        pending,
                        follow_up=attempt + 1,
                    )
                    if not should_retry:
                        break
                if restarted:
                    continue
                break

    async def _run_reactive_attempt(
        self,
        agent: AgentConfig,
        pending: ConversationMessage,
        *,
        follow_up: int = 0,
    ) -> bool:
        reactive_payload = {
            "reactive": True,
            "message_id": pending.message_id,
        }
        if follow_up:
            reactive_payload["follow_up"] = follow_up
        await self.dispatch_command(
            CommandEnvelope(
                subject=f"agent.command.{agent.id}.generate.tick",
                payload=reactive_payload,
            )
        )
        newer = self._drain_latest_reactive_message(self._reactive_mailboxes[agent.id], agent_id=agent.id)
        if newer is not None:
            await self._reactive_mailboxes[agent.id].put(newer)
            return True
        if self.registry.active_reservation_for(agent.id) is not None:
            return False
        await self.dispatch_command(
            CommandEnvelope(
                subject=f"agent.command.{agent.id}.schedule.tick",
                payload=reactive_payload,
            )
        )
        return self.registry.run_state() == "running" and self.registry.active_reservation_for(agent.id) is None

    async def _coalesce_reactive_messages(
        self,
        agent: AgentConfig,
        *,
        index: int,
        queue: asyncio.Queue[ConversationMessage],
        pending: ConversationMessage,
    ) -> ConversationMessage:
        delay = self._reactive_schedule_delay(agent, index=index)
        while True:
            newer = await self._wait_for_reactive_message(queue, timeout=delay)
            if newer is None:
                return pending
            if newer.sender_id == agent.id:
                continue
            pending = newer

    async def _wait_for_reactive_message(
        self,
        queue: asyncio.Queue[ConversationMessage],
        *,
        timeout: float,
    ) -> ConversationMessage | None:
        try:
            message = await asyncio.wait_for(queue.get(), timeout=timeout)
        except TimeoutError:
            return None
        queue.task_done()
        return message

    def _drain_latest_reactive_message(
        self,
        queue: asyncio.Queue[ConversationMessage],
        *,
        agent_id: str,
    ) -> ConversationMessage | None:
        latest: ConversationMessage | None = None
        while True:
            with contextlib.suppress(asyncio.QueueEmpty):
                message = queue.get_nowait()
                queue.task_done()
                if message.sender_id != agent_id:
                    latest = message
                continue
            return latest

    async def _wait_for_buffer_update(
        self,
        agent_id: str,
        *,
        previous_version: int,
        timeout: float,
    ) -> bool:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if self.registry.run_state() != "running":
                return False
            if self.registry.buffer_version_for(agent_id) > previous_version:
                return True
            await asyncio.sleep(0.01)
        return self.registry.buffer_version_for(agent_id) > previous_version


def load_config(path: Path) -> AppConfig:
    text = path.read_text()
    if path.suffix == ".json":
        data = json.loads(text)
    else:
        try:
            import yaml  # type: ignore
        except ImportError as exc:  # pragma: no cover - optional dependency
            raise RuntimeError("YAML config requires PyYAML") from exc
        data = yaml.safe_load(text)
    return AppConfig.model_validate(data)
