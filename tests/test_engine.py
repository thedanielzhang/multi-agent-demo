from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from mafia.config import ModeProfile
from mafia.engine import ConversationEngine, load_config
from mafia.event_log import EventLog
from mafia.messages import AgentContextSnapshot, AgentTopicSnapshot, AnalyzerReply, CommandEnvelope, LoggedEvent, RoomMetricsSnapshot, TopicSummary
from mafia.messages import CandidateRecord, ConversationMessage
from mafia.policies import PolicySet
from mafia.projections import ProjectionRegistry
from mafia.runtimes import ScriptedAgentRuntime
from mafia.transport import register_transport

ROOT = Path(__file__).resolve().parents[1]


async def _shutdown(engine: ConversationEngine) -> list[LoggedEvent]:
    if engine.registry.run_state() not in {"stopped", "failed"}:
        await engine.dispatch_command(CommandEnvelope(subject="run.command.stop"))
    events = await engine.export_events()
    await engine.close()
    return events


async def _wait_for_event(
    engine: ConversationEngine,
    predicate,
    *,
    timeout: float = 2.0,
) -> LoggedEvent:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        events = await engine.export_events()
        for logged in events:
            if predicate(logged):
                return logged
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("event predicate was not satisfied in time")
        await asyncio.sleep(0.02)


class CapturingRuntime(ScriptedAgentRuntime):
    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[str, str]] = []

    async def invoke(self, role, prompt: str, **kwargs):
        self.calls.append((role.name, prompt))
        return await super().invoke(role, prompt, **kwargs)


class FailingSchedulerRuntime(ScriptedAgentRuntime):
    def __init__(self, failures_before_success: int) -> None:
        super().__init__()
        self.failures_before_success = failures_before_success

    async def invoke(self, role, prompt: str, **kwargs):
        if role.name == "scheduler" and self.failures_before_success > 0:
            self.failures_before_success -= 1
            raise RuntimeError("scheduler boom")
        return await super().invoke(role, prompt, **kwargs)


class SlowGeneratorRuntime(ScriptedAgentRuntime):
    def __init__(self, delay_seconds: float) -> None:
        super().__init__()
        self.delay_seconds = delay_seconds

    async def invoke(self, role, prompt: str, **kwargs):
        if role.name == "generator":
            await asyncio.sleep(self.delay_seconds)
        return await super().invoke(role, prompt, **kwargs)


@pytest.mark.asyncio
async def test_append_then_publish_updates_projection_before_event_observer(improved_config):
    engine = ConversationEngine(improved_config)
    observed = asyncio.Event()

    async def handler(_subject, event):
        if event.subject == "conversation.event.message.committed":
            assert engine.registry.has_client_message_id(event.payload["client_message_id"])
            observed.set()

    engine.bus.subscribe("conversation.event.message.committed", handler, maxsize=16, overflow="block")
    await engine.start()
    await engine.submit_message(text="hello team")
    await asyncio.wait_for(observed.wait(), timeout=1.0)
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_duplicate_client_message_is_suppressed(baseline_config):
    engine = ConversationEngine(baseline_config)
    await engine.start()
    command = CommandEnvelope(
        subject="conversation.command.message.submit",
        payload={
            "client_message_id": "same-id",
            "sender_id": "human",
            "sender_kind": "human",
            "display_name": "Human",
            "text": "same message",
        },
    )
    await engine.dispatch_command(command)
    await engine.dispatch_command(
        CommandEnvelope(
            subject="conversation.command.message.submit",
            payload=dict(command.payload),
        )
    )
    events = await _shutdown(engine)
    committed = [event for event in events if event.event.subject == "conversation.event.message.committed"]
    assert len(committed) == 1


@pytest.mark.asyncio
async def test_run_start_event_records_mode_runtime_transport_and_policy_profile(improved_config):
    engine = ConversationEngine(improved_config)
    await engine.start()
    events = await engine.export_events()
    started = next(logged for logged in events if logged.event.subject == "run.event.started")
    assert started.event.payload["mode"] == ModeProfile.IMPROVED_BUFFERED_ASYNC
    assert started.event.payload["runtime_provider"] == "scripted"
    assert started.event.payload["transport_provider"] == "loopback"
    assert started.event.payload["policy_profile"]["scheduling_policy"] == "buffered.async"
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_baseline_mode_is_serial_and_uses_paper_style_prompt(baseline_config):
    runtime = CapturingRuntime()
    engine = ConversationEngine(baseline_config, runtime=runtime)
    await engine.start()
    await engine.submit_message(text="where should we eat?")
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject.endswith(".candidate.generated"),
    )
    events = await _shutdown(engine)

    subjects = [logged.event.subject for logged in events]
    assert not any(subject.endswith(".candidate.buffered") for subject in subjects)
    assert not any(subject.startswith("topic.event.") for subject in subjects)

    send_seq_by_agent = {
        logged.event.payload["agent_id"]: logged.seq
        for logged in events
        if logged.event.subject.endswith(".scheduler.decided")
        and logged.event.payload["decision"] == "send"
    }
    for logged in events:
        if not logged.event.subject.endswith(".candidate.generated"):
            continue
        assert logged.seq > send_seq_by_agent[logged.event.payload["agent_id"]]

    scheduler_prompts = [prompt for role_name, prompt in runtime.calls if role_name == "scheduler"]
    assert scheduler_prompts
    prompt = scheduler_prompts[0]
    assert "Conversation history with timestamps" in prompt
    assert "Goals:" in prompt
    assert "Current pacing mode:" in prompt


def test_scheduler_input_switches_to_listening_when_agent_rate_exceeds_average(baseline_config):
    policies = PolicySet(baseline_config)
    agent = baseline_config.agents[0]
    current_time = __import__("datetime").datetime.now(__import__("datetime").UTC)
    view = AgentContextSnapshot(
        agent_id=agent.id,
        watermark=0,
        current_time=current_time,
        recent_messages=[],
        focus_messages=[],
        room_metrics=RoomMetricsSnapshot(active_participant_count=3, avg_message_rate=1 / 3),
        active_participant_count=3,
        agent_message_rate=0.6,
        avg_message_rate=1 / 3,
        time_since_last_any=0.0,
        time_since_last_own=0.0,
        buffer_size=0,
        buffer_version=0,
        run_state="running",
    )
    scheduler_input = policies.scheduler_input(agent, view)
    assert scheduler_input.talk_mode == "listening"


@pytest.mark.asyncio
async def test_improved_mode_uses_transport_and_shared_conversation_boundary(improved_config):
    engine = ConversationEngine(improved_config)
    await engine.start()
    await engine.submit_message(text="thai sounds good to me")
    ack = await _wait_for_event(engine, lambda logged: logged.event.subject == "transport.event.message.acked")
    events = await _shutdown(engine)
    committed = next(
        logged.event.payload
        for logged in events
        if logged.event.subject == "conversation.event.message.committed"
        and logged.event.payload["client_message_id"] == ack.event.payload["client_message_id"]
    )
    assert committed["metadata"]["transport"] == "loopback"
    assert committed["metadata"]["candidate_id"] == ack.event.payload["candidate_id"]
    assert committed["metadata"]["reservation_id"] == ack.event.payload["reservation_id"]


@pytest.mark.asyncio
async def test_single_agent_cannot_accumulate_overlapping_reservations(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.chat.max_duration_seconds = 5.0
    config.chat.typing_words_per_second = 0.5
    engine = ConversationEngine(config)
    await engine.start()
    await engine.submit_message(text="let's pick lunch")
    first_reserved = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject.endswith(".candidate.reserved"),
        timeout=2.0,
    )
    await asyncio.sleep(0.6)
    events = await engine.export_events()
    reserved = [
        logged
        for logged in events
        if logged.event.subject.endswith(".candidate.reserved")
        and logged.event.payload["agent_id"] == config.agents[0].id
    ]
    assert first_reserved.event.payload["agent_id"] == config.agents[0].id
    assert len(reserved) == 1
    await _shutdown(engine)


def test_scheduler_input_exposes_duplicate_suppression_signal(improved_config):
    policies = PolicySet(improved_config)
    agent = improved_config.agents[0]
    current_time = __import__("datetime").datetime.now(__import__("datetime").UTC)
    recent_message = ConversationMessage(
        message_id="m1",
        client_message_id="c1",
        sender_id="other",
        sender_kind="agent",
        display_name="Jordan",
        text="thai sounds good to me",
        created_at=current_time,
        sequence_no=1,
    )
    context = AgentContextSnapshot(
        agent_id=agent.id,
        watermark=1,
        current_time=current_time,
        recent_messages=[recent_message],
        focus_messages=[recent_message],
        focus_message_ids=["m1"],
        room_metrics=RoomMetricsSnapshot(
            watermark=1,
            active_participant_count=2,
            avg_message_rate=0.5,
            agent_message_rates={"other": 0.5},
            recent_keyword_sketch=["thai", "lunch"],
        ),
        active_participant_count=2,
        agent_message_rate=0.0,
        avg_message_rate=0.5,
        time_since_last_any=0.1,
        time_since_last_own=2.0,
        buffer_size=1,
        buffer_version=1,
        run_state="running",
    )
    candidate = CandidateRecord(
        candidate_id="cand-1",
        agent_id=agent.id,
        text="thai sounds good to me",
        created_at=current_time,
    )
    scheduler_input = policies.scheduler_input(agent, context, buffer_candidates=[candidate], active_reservations=[])
    assert scheduler_input.candidate_preview_text == "thai sounds good to me"
    assert scheduler_input.candidate_similarity_score >= 0.99
    assert scheduler_input.similar_recent_message_text == "thai sounds good to me"


@pytest.mark.asyncio
async def test_agents_are_not_blocked_by_room_level_active_reservation(improved_config):
    config = improved_config.model_copy(deep=True)
    config.chat.max_duration_seconds = 5.0
    config.chat.typing_words_per_second = 0.5
    for agent in config.agents:
        agent.scheduler.tick_rate_seconds = 0.1
        agent.generation.tick_rate_seconds = 0.1
        agent.personality.talkativeness = 1.0
        agent.personality.confidence = 1.0
    engine = ConversationEngine(config)
    await engine.start()
    await engine.submit_message(text="let's decide lunch")
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject.endswith(".candidate.reserved"),
        timeout=2.0,
    )
    await asyncio.sleep(0.4)
    events = await engine.export_events()
    assert not any(
        logged.event.subject.endswith(".scheduler.decided")
        and logged.event.payload.get("reason") == "room_active_reservation"
        for logged in events
    )
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_reactive_nudge_triggers_scheduler_before_coarse_tick(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.agents[0].scheduler.tick_rate_seconds = 5.0
    config.agents[0].generation.tick_rate_seconds = 5.0
    config.agents[0].personality.reactivity = 1.0
    config.chat.max_duration_seconds = 3.0
    engine = ConversationEngine(config)
    await engine.start()
    await engine.submit_message(text="any lunch ideas?")
    decided = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject.endswith(".scheduler.decided")
        and logged.event.payload["agent_id"] == config.agents[0].id,
        timeout=1.0,
    )
    committed = next(
        logged
        for logged in await engine.export_events()
        if logged.event.subject == "conversation.event.message.committed"
    )
    assert decided.seq > committed.seq
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_improved_mode_can_open_conversation_without_human_message(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.chat.typing_words_per_second = 100.0
    engine = ConversationEngine(config)
    await engine.start()
    opening = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "conversation.event.message.committed"
        and logged.event.payload["sender_kind"] == "agent",
        timeout=2.0,
    )
    assert opening.event.payload["sender_id"] == config.agents[0].id
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_improved_mode_scheduler_has_async_heartbeat_without_new_messages(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.agents[0].personality.talkativeness = 0.0
    config.agents[0].personality.confidence = 0.0
    config.agents[0].scheduler.tick_rate_seconds = 0.15
    config.agents[0].generation.tick_rate_seconds = 0.05
    config.chat.typing_words_per_second = 100.0
    engine = ConversationEngine(config)
    await engine.start()
    await asyncio.sleep(0.75)
    events = await engine.export_events()
    scheduler_decisions = [
        logged
        for logged in events
        if logged.event.subject.endswith(".scheduler.decided")
        and logged.event.payload["agent_id"] == config.agents[0].id
    ]
    assert len(scheduler_decisions) >= 2
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_improved_mode_bootstrap_still_opens_with_slow_generator(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.agents[0].generation.tick_rate_seconds = 10.0
    config.chat.typing_words_per_second = 100.0
    engine = ConversationEngine(config, runtime=SlowGeneratorRuntime(delay_seconds=0.45))
    await engine.start()
    opening = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "conversation.event.message.committed"
        and logged.event.payload["sender_kind"] == "agent",
        timeout=2.5,
    )
    assert opening.event.payload["sender_id"] == config.agents[0].id
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_reactive_scheduler_coalesces_quick_messages_to_latest_context(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.agents[0].scheduler.tick_rate_seconds = 5.0
    config.agents[0].generation.tick_rate_seconds = 5.0
    config.agents[0].personality.reactivity = 1.0
    engine = ConversationEngine(config)
    await engine.start()
    await engine.submit_message(text="what should we eat?")
    await engine.submit_message(text="maybe thai or sushi")
    decided = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject.endswith(".scheduler.decided")
        and logged.event.payload["agent_id"] == config.agents[0].id,
        timeout=1.5,
    )
    committed = [
        logged
        for logged in await engine.export_events()
        if logged.event.subject == "conversation.event.message.committed"
    ]
    assert len(committed) >= 2
    latest_committed = committed[-1]
    assert decided.event.payload["source_watermark"] >= latest_committed.seq
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_reactive_send_path_survives_slow_generator_projection_lag(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.agents[0].scheduler.tick_rate_seconds = 5.0
    config.agents[0].generation.tick_rate_seconds = 5.0
    config.agents[0].personality.reactivity = 1.0
    config.chat.typing_words_per_second = 100.0
    engine = ConversationEngine(config, runtime=SlowGeneratorRuntime(delay_seconds=0.35))
    await engine.start()
    await engine.submit_message(text="what sounds good for lunch?")
    reply = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "conversation.event.message.committed"
        and logged.event.payload["sender_kind"] == "agent",
        timeout=2.5,
    )
    assert reply.event.payload["sender_id"] == config.agents[0].id
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_analyzer_uses_committed_messages_only_and_topic_ids_persist(improved_config):
    engine = ConversationEngine(improved_config)
    await engine.start()
    await engine.submit_message(text="lunch thai budget")
    first = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "topic.event.alex.snapshot.updated",
    )
    second = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "topic.event.alex.snapshot.updated" and logged.seq > first.seq,
        timeout=3.5,
    )
    events = await _shutdown(engine)
    committed_ids = {
        logged.event.payload["message_id"]
        for logged in events
        if logged.event.subject == "conversation.event.message.committed"
    }
    assert set(first.event.payload["window_message_ids"]).issubset(committed_ids)
    assert set(second.event.payload["window_message_ids"]).issubset(committed_ids)
    assert first.event.payload["dominant_topic_id"] == second.event.payload["dominant_topic_id"]


@pytest.mark.asyncio
async def test_pause_freezes_scheduled_delivery_until_resume(improved_config):
    config = improved_config.model_copy(deep=True)
    config.chat.typing_words_per_second = 1.5
    config.chat.max_duration_seconds = 6.0
    engine = ConversationEngine(config)
    await engine.start()
    await engine.submit_message(text="thai sounds good")
    scheduled = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject.endswith(".delivery.scheduled"),
    )
    reservation_id = scheduled.event.payload["reservation_id"]
    await engine.dispatch_command(CommandEnvelope(subject="run.command.pause"))
    await asyncio.sleep(0.5)
    paused_events = await engine.export_events()
    assert not any(
        logged.event.subject.endswith(".delivery.submitted")
        and logged.event.payload["reservation_id"] == reservation_id
        for logged in paused_events
    )
    await engine.dispatch_command(CommandEnvelope(subject="run.command.resume"))
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject.endswith(".delivery.submitted")
        and logged.event.payload["reservation_id"] == reservation_id,
        timeout=4.0,
    )
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_retryable_transport_failure_requeues_candidate(improved_config):
    provider = "test-retryable-fail"

    class RetryableFailTransport:
        name = provider

        def __init__(self, engine, config):
            self.engine = engine

        async def handle_send(self, command: CommandEnvelope) -> None:
            await self.engine.append_event(
                make_event(
                    "transport.event.message.failed",
                    command=command,
                    payload={
                        "reservation_id": command.payload["reservation_id"],
                        "candidate_id": command.payload["candidate_id"],
                        "reason": "retryable",
                        "retryable": True,
                    },
                )
            )
            await self.engine.dispatch_command(
                CommandEnvelope(
                    subject=f"agent.command.{command.payload['agent_id']}.delivery.transport_failed",
                    correlation_id=command.correlation_id,
                    causation_id=command.command_id,
                    payload={
                        "reservation_id": command.payload["reservation_id"],
                        "retryable": True,
                        "reason": "retryable",
                    },
                )
            )

    from mafia.messages import make_event

    register_transport(provider, lambda engine, config: RetryableFailTransport(engine, config))
    config = improved_config.model_copy(deep=True)
    config.transport.provider = provider
    engine = ConversationEngine(config)
    await engine.start()
    await engine.submit_message(text="pizza maybe")
    requeued = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject.endswith(".candidate.requeued"),
        timeout=2.0,
    )
    assert engine.registry.buffer_for(requeued.event.payload["candidate"]["agent_id"])
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_replay_reconstructs_projection_state_without_rerunning_inference(improved_config):
    engine = ConversationEngine(improved_config)
    await engine.start()
    await engine.submit_message(text="sushi?")
    await _wait_for_event(engine, lambda logged: logged.event.subject == "transport.event.message.acked")
    original_events = await _shutdown(engine)

    replay_log = EventLog()
    replay_registry = ProjectionRegistry(replay_log, improved_config)
    await replay_registry.start()
    for logged in original_events:
        await replay_log.append(logged.event)
    await replay_registry.wait_until(replay_log.latest_seq)
    assert replay_registry.run_state() == "stopped"
    assert len(replay_registry.latest_messages()) == len(
        [event for event in original_events if event.event.subject == "conversation.event.message.committed"]
    )
    await replay_registry.close()


@pytest.mark.asyncio
async def test_failed_precommit_submit_can_retry_same_client_message_id(baseline_config, monkeypatch):
    engine = ConversationEngine(baseline_config)
    await engine.start()
    command = CommandEnvelope(
        subject="conversation.command.message.submit",
        payload={
            "client_message_id": "retry-id",
            "sender_id": "human",
            "sender_kind": "human",
            "display_name": "Human",
            "text": "retry me",
        },
    )

    original_append = engine.append_event
    failed = {"done": False}

    async def fail_once(event):
        if event.subject == "conversation.event.message.committed" and not failed["done"]:
            failed["done"] = True
            raise RuntimeError("boom before commit")
        return await original_append(event)

    monkeypatch.setattr(engine, "append_event", fail_once)
    with pytest.raises(RuntimeError, match="boom before commit"):
        await engine._handle_submit_message(command)  # noqa: SLF001

    monkeypatch.setattr(engine, "append_event", original_append)
    await engine._handle_submit_message(command)  # noqa: SLF001
    events = await _shutdown(engine)
    committed = [event for event in events if event.event.subject == "conversation.event.message.committed"]
    assert len(committed) == 1
    assert committed[0].event.payload["client_message_id"] == "retry-id"


@pytest.mark.asyncio
async def test_worker_failures_are_consecutive_not_cumulative(baseline_config):
    config = baseline_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    runtime = FailingSchedulerRuntime(failures_before_success=2)
    engine = ConversationEngine(config, runtime=runtime)
    await engine.start()
    await engine.dispatch_command(CommandEnvelope(subject=f"agent.command.{config.agents[0].id}.schedule.tick"))
    assert engine.registry.run_state() == "running"
    events = await _shutdown(engine)
    failures = [event for event in events if event.event.subject == "debug.event.worker.failed"]
    assert len(failures) == 2


@pytest.mark.asyncio
async def test_agent_call_debug_events_capture_scheduler_and_generator(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    engine = ConversationEngine(config)
    await engine.start()
    await engine.submit_message(text="thai sounds good")
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.call.completed"
        and logged.event.payload["worker_kind"] == "scheduler",
        timeout=2.0,
    )
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.call.completed"
        and logged.event.payload["worker_kind"] == "generator",
        timeout=2.0,
    )
    events = await _shutdown(engine)
    completed = [
        logged.event.payload
        for logged in events
        if logged.event.subject == "debug.event.agent.call.completed"
    ]
    scheduler = next(item for item in completed if item["worker_kind"] == "scheduler")
    generator = next(item for item in completed if item["worker_kind"] == "generator")
    assert "input_summary" in scheduler
    assert "output_summary" in scheduler
    assert "talk_mode" in scheduler["input_summary"]
    assert "decision" in scheduler["output_summary"]
    assert "focus_preview" in generator["input_summary"]
    assert "text" in generator["output_summary"]


@pytest.mark.asyncio
async def test_three_consecutive_worker_failures_fail_run(baseline_config):
    config = baseline_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    runtime = FailingSchedulerRuntime(failures_before_success=3)
    engine = ConversationEngine(config, runtime=runtime)
    await engine.start()
    with pytest.raises(RuntimeError, match="scheduler boom"):
        await engine.dispatch_command(CommandEnvelope(subject=f"agent.command.{config.agents[0].id}.schedule.tick"))
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "run.event.state.changed" and logged.event.payload["state"] == "failed",
        timeout=1.0,
    )
    await engine.close()


def test_per_agent_memory_decay_override_affects_topic_memory(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents[0].context = config.context_defaults.model_copy(update={"memory_decay": 0.2})
    config.agents[1].context = config.context_defaults.model_copy(update={"memory_decay": 0.9})
    policies = PolicySet(config)
    previous = AgentTopicSnapshot(
        snapshot_id="snap-1",
        agent_id="agent",
        watermark=1,
        window_message_ids=[],
        topics=[],
        dominant_topic_id=None,
        generated_at=__import__("datetime").datetime.now(__import__("datetime").UTC),
        stale_after=__import__("datetime").datetime.now(__import__("datetime").UTC),
        memory_summary={"topic-lunch": 1.0},
    )
    reply = AnalyzerReply(
        topics=[TopicSummary(topic_id="topic-lunch", label="lunch", keywords=["lunch"], weight=0.5, confidence=0.8)]
    )
    _, _, alex_memory = policies.reconcile_topics(config.agents[0], previous, reply)
    _, _, jordan_memory = policies.reconcile_topics(config.agents[1], previous, reply)
    assert alex_memory["topic-lunch"] == pytest.approx(0.7)
    assert jordan_memory["topic-lunch"] == pytest.approx(1.4)


def test_load_config_supports_yaml(tmp_path, baseline_config):
    yaml_text = """
mode: baseline.time_to_talk
runtime:
  provider: scripted
chat:
  scenario: "Lunch debate"
agents:
  - id: alex
    display_name: Alex
"""
    path = tmp_path / "config.yaml"
    path.write_text(yaml_text)
    config = load_config(path)
    assert config.chat.scenario == "Lunch debate"
    assert config.runtime.provider == "scripted"


def test_unsupported_runtime_provider_fails_fast(baseline_config):
    config = baseline_config.model_copy(deep=True)
    config.runtime.provider = "not-a-runtime"
    with pytest.raises(ValueError, match="Unsupported runtime"):
        ConversationEngine(config)


def test_cli_smoke_with_json_config(tmp_path, baseline_config):
    config_path = tmp_path / "config.json"
    data = baseline_config.model_dump(mode="json")
    data["chat"]["max_duration_seconds"] = 0.4
    data["chat"]["max_messages"] = 4
    config_path.write_text(json.dumps(data))
    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT / "src")
    result = subprocess.run(
        [sys.executable, str(ROOT / "src" / "mafia" / "cli.py"), str(config_path), "--message", "hello"],
        capture_output=True,
        text=True,
        env=env,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert "run.event.started" in result.stdout


@pytest.mark.asyncio
async def test_housekeeping_does_not_stop_unbounded_room(baseline_config):
    config = baseline_config.model_copy(deep=True)
    config.agents = []
    config.chat.max_duration_seconds = None
    config.chat.max_messages = None
    engine = ConversationEngine(config)
    await engine.start()
    await asyncio.sleep(0.6)
    assert engine.registry.run_state() == "running"
    await _shutdown(engine)
