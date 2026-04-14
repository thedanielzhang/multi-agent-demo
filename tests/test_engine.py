from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from datetime import timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from mafia.config import ModeProfile, RoomMode
from mafia.agent import AgentInvoker
from mafia.compose_compat import Workspace
from mafia.engine import ConversationEngine, load_config
from mafia.event_log import EventLog
from mafia.messages import AgentContextSnapshot, AgentTopicSnapshot, AnalyzerReply, CommandEnvelope, LoggedEvent, MafiaFaction, MafiaGameSnapshot, MafiaGameStatus, MafiaPhase, MafiaPlayerRecord, MafiaPrivateState, MafiaPublicState, MafiaRole, RoomMetricsSnapshot, SchedulerInputSnapshot, SchedulerReply, TopicSummary, make_event, utc_now
from mafia.mafia_controller import MafiaGameController, mafia_count_for_players
from mafia.messages import CandidateRecord, CommitmentState, ConversationMessage, DeliveryReservation, OpenQuestionState, ProposalState, ResponseSlotState, RoomDiscourseStateSnapshot
from mafia.mafia_personas import generate_mafia_personas
from mafia.policies import PolicySet
from mafia.projections import ProjectionRegistry
from mafia.runtimes import ScriptedAgentRuntime
from mafia.scripted_logic import ScriptedAgentLogic
from mafia.transport import register_transport
from mafia.workers import AgentDeliveryWorker

ROOT = Path(__file__).resolve().parents[1]


def _message(
    *,
    message_id: str,
    client_message_id: str,
    sender_id: str,
    display_name: str,
    text: str,
    created_at,
    sequence_no: int,
    mentions: list[str] | None = None,
    reply_hint: str | None = None,
    sender_kind="human",
) -> ConversationMessage:
    return ConversationMessage(
        message_id=message_id,
        client_message_id=client_message_id,
        sender_id=sender_id,
        sender_kind=sender_kind,
        display_name=display_name,
        text=text,
        created_at=created_at,
        sequence_no=sequence_no,
        mentions=mentions or [],
        reply_hint=reply_hint,
    )


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


async def _discard_agent_buffer(engine: ConversationEngine, agent_id: str) -> None:
    for candidate in list(engine.registry.buffer_for(agent_id)):
        await engine.append_event_and_wait(
            make_event(
                f"agent.event.{agent_id}.candidate.discarded",
                payload={
                    "agent_id": agent_id,
                    "candidate_id": candidate.candidate_id,
                    "reason": "test_clear",
                },
            )
        )


def _mafia_day_discussion_snapshot(agent, *, round_no: int = 1) -> MafiaGameSnapshot:
    started_at = utc_now()
    return MafiaGameSnapshot(
        game_status=MafiaGameStatus.ACTIVE,
        phase=MafiaPhase.DAY_DISCUSSION,
        phase_started_at=started_at,
        phase_ends_at=started_at + timedelta(seconds=45),
        total_players=1,
        round_no=round_no,
        players=[
            MafiaPlayerRecord(
                participant_id=agent.id,
                display_name=agent.display_name,
                is_human=False,
                seat_index=0,
                role=MafiaRole.TOWN,
                faction=MafiaFaction.TOWN,
                connected=True,
            )
        ],
    )


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


class SlowSchedulerRuntime(ScriptedAgentRuntime):
    def __init__(self, delay_seconds: float) -> None:
        super().__init__()
        self.delay_seconds = delay_seconds

    async def invoke(self, role, prompt: str, **kwargs):
        if role.name == "scheduler":
            await asyncio.sleep(self.delay_seconds)
        return await super().invoke(role, prompt, **kwargs)


class CountingSlowGeneratorRuntime(ScriptedAgentRuntime):
    def __init__(self, delay_seconds: float) -> None:
        super().__init__()
        self.delay_seconds = delay_seconds
        self.scheduler_calls = 0
        self.generator_calls = 0

    async def invoke(self, role, prompt: str, **kwargs):
        if role.name == "scheduler":
            self.scheduler_calls += 1
        if role.name == "generator":
            self.generator_calls += 1
            await asyncio.sleep(self.delay_seconds)
        return await super().invoke(role, prompt, **kwargs)


class SequencedGeneratorRuntime(ScriptedAgentRuntime):
    def __init__(self, delays: list[float], *, wait_on_buffered_scheduler: bool = False) -> None:
        super().__init__()
        self.delays = delays or [0.0]
        self.wait_on_buffered_scheduler = wait_on_buffered_scheduler
        self.generator_calls = 0
        self.scheduler_calls = 0

    async def invoke(self, role, prompt: str, **kwargs):
        if role.name == "scheduler":
            self.scheduler_calls += 1
            if self.wait_on_buffered_scheduler:
                snapshot = SchedulerInputSnapshot.model_validate(self._parse_input(prompt))
                if snapshot.has_buffered_candidate:
                    return SchedulerReply(decision="wait", reason="buffered-not-relevant")
        if role.name == "generator":
            delay_index = min(self.generator_calls, len(self.delays) - 1)
            delay_seconds = self.delays[delay_index]
            self.generator_calls += 1
            if delay_seconds > 0:
                await asyncio.sleep(delay_seconds)
        return await super().invoke(role, prompt, **kwargs)


class RecordingRuntime(ScriptedAgentRuntime):
    def __init__(self) -> None:
        super().__init__()
        self.calls: list[tuple[str, str, str | None, str | None]] = []

    async def invoke(self, role, prompt: str, **kwargs):
        workspace = kwargs.get("workspace")
        self.calls.append(
            (
                role.metadata.get("agent_id", "agent"),
                role.name,
                kwargs.get("session_key"),
                str(workspace.path) if workspace is not None else None,
            )
        )
        return await super().invoke(role, prompt, **kwargs)


class SlowAnalyzerRuntime(ScriptedAgentRuntime):
    def __init__(self, delay_seconds: float) -> None:
        super().__init__()
        self.delay_seconds = delay_seconds

    async def invoke(self, role, prompt: str, **kwargs):
        if role.name == "analyzer":
            await asyncio.sleep(self.delay_seconds)
        return await super().invoke(role, prompt, **kwargs)


class SlowVoterRuntime(ScriptedAgentRuntime):
    def __init__(self, delay_seconds: float) -> None:
        super().__init__()
        self.delay_seconds = delay_seconds

    async def invoke(self, role, prompt: str, **kwargs):
        if role.name == "voter":
            await asyncio.sleep(self.delay_seconds)
        return await super().invoke(role, prompt, **kwargs)


class DeliveryTestEngine:
    def __init__(self, config, registry, event_log) -> None:
        self.config = config
        self.registry = registry
        self.event_log = event_log
        self.policies = PolicySet(config)
        self.commands: list[CommandEnvelope] = []

    async def append_event(self, event):
        logged = await self.event_log.append(event)
        await self.registry.wait_until(logged.seq)
        return logged

    async def dispatch_command(self, command: CommandEnvelope) -> None:
        self.commands.append(command)


@pytest.mark.asyncio
async def test_append_then_publish_updates_projection_before_event_observer(improved_config):
    engine = ConversationEngine(improved_config)
    observed = asyncio.Event()

    async def handler(_subject, logged: LoggedEvent):
        if logged.event.subject == "conversation.event.message.committed":
            assert engine.registry.has_client_message_id(logged.event.payload["client_message_id"])
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


def test_seeded_mafia_personas_include_chatroom_style_guidance():
    personas = generate_mafia_personas("table-7", 5)
    assert len(personas) == 5
    for persona in personas:
        assert "live group chat" in persona.style_prompt
        assert "stage directions" in persona.style_prompt
        assert persona.personality.reactivity >= 1.0
        assert persona.personality.talkativeness >= 0.88
        assert persona.personality.confidence >= 0.88
        assert persona.generation.buffer_size == 1
        assert persona.generation.staleness_window_seconds == 7.0


def test_seeded_mafia_personas_cover_distinct_interaction_vectors_before_repeating():
    personas = generate_mafia_personas("vector-table", 8)
    archetype_markers = {persona.goals[-1] for persona in personas}
    talkativeness = [persona.personality.talkativeness for persona in personas]
    confidence = [persona.personality.confidence for persona in personas]
    topic_loyalty = [persona.personality.topic_loyalty for persona in personas]

    assert len(archetype_markers) == 8
    assert max(talkativeness) - min(talkativeness) >= 0.12
    assert max(confidence) - min(confidence) >= 0.12
    assert max(topic_loyalty) - min(topic_loyalty) >= 0.5


def test_mafia_generator_prompt_reinforces_chatroom_message_style(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    policies = PolicySet(config)
    context = AgentContextSnapshot(
        agent_id=config.agents[0].id,
        watermark=3,
        current_time=utc_now(),
        recent_messages=[],
        focus_messages=[],
        room_metrics=RoomMetricsSnapshot(),
    )
    prompt = policies.prompts.generator_prompt(
        config.agents[0],
        policies.generator_input(config.agents[0], context),
    )
    assert "real player in a live chat room" in prompt
    assert "No stage directions" in prompt


def test_mafia_generator_prompt_includes_private_role_context(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    policies = PolicySet(config)
    context = AgentContextSnapshot(
        agent_id=config.agents[0].id,
        watermark=3,
        current_time=utc_now(),
        recent_messages=[],
        focus_messages=[],
        room_metrics=RoomMetricsSnapshot(),
    )
    prompt = policies.prompts.generator_prompt(
        config.agents[0],
        policies.generator_input(
            config.agents[0],
            context,
            mafia_public_state=MafiaPublicState(game_status=MafiaGameStatus.ACTIVE, phase=MafiaPhase.DAY_DISCUSSION),
            mafia_private_state=MafiaPrivateState(
                participant_id=config.agents[0].id,
                role="mafia",
                faction=MafiaFaction.MAFIA,
                alive=True,
                spectator=False,
                teammates=["teammate-2"],
            ),
        ),
    )
    assert "Your private role: mafia" in prompt
    assert "Known mafia teammates: teammate-2" in prompt
    assert "This message is public table chat" in prompt


def test_mafia_pre_day_spinup_candidates_stay_fresh_until_day_opens(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    config.mafia.night_reveal_seconds = 30.0
    policies = PolicySet(config)
    agent = config.agents[0]
    now = utc_now()
    candidate = CandidateRecord(
        candidate_id="pre-day-1",
        agent_id=agent.id,
        text="we should compare notes before voting again",
        created_at=now - timedelta(seconds=25),
        metadata={"mafia_pre_day_spinup": True},
    )
    assert policies.candidate_is_stale(agent, candidate, now) is False


def test_mafia_vote_resolution_reveals_eliminated_faction():
    controller = MafiaGameController(SimpleNamespace())
    snapshot = MafiaGameSnapshot(
        game_status=MafiaGameStatus.ACTIVE,
        phase=MafiaPhase.DAY_VOTE,
        players=[
            MafiaPlayerRecord(participant_id="p1", display_name="Alex", is_human=True, seat_index=0, role=MafiaRole.MAFIA, faction=MafiaFaction.MAFIA),
            MafiaPlayerRecord(participant_id="p2", display_name="Jordan", is_human=True, seat_index=1, role=MafiaRole.TOWN, faction=MafiaFaction.TOWN),
        ],
        day_votes={"p2": "p1"},
    )
    next_snapshot, revealed = controller._resolve_day_vote(snapshot)
    assert revealed["eliminated_participant_id"] == "p1"
    assert revealed["eliminated_faction"] == "mafia"
    assert "They were mafia" in revealed["summary"]
    assert next_snapshot.revealed_eliminations[-1].faction == MafiaFaction.MAFIA


@pytest.mark.asyncio
async def test_mafia_start_game_avoids_duplicate_seeded_display_names(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    config.mafia.total_players = 5
    config.agents = generate_mafia_personas("duped-table", 5)
    config.agents[1].display_name = config.agents[0].display_name
    engine = ConversationEngine(config)
    await engine.start()
    await engine.dispatch_command(
        CommandEnvelope(
            subject="mafia.command.game.start",
            payload={"humans": []},
        )
    )
    snapshot = engine.registry.mafia_snapshot()
    assert snapshot is not None
    names = [player.display_name.casefold() for player in snapshot.players]
    assert len(names) == len(set(names))
    await _shutdown(engine)


def test_mafia_vote_prompt_does_not_label_players_as_human_or_agent(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    policies = PolicySet(config)
    prompt = policies.prompts.mafia_vote_prompt(
        config.agents[0],
        policies.mafia_vote_input(
            config.agents[0],
            MafiaPublicState(
                game_status=MafiaGameStatus.ACTIVE,
                phase=MafiaPhase.DAY_VOTE,
                roster=[
                    {"participant_id": "alex", "display_name": "Alex", "is_human": True, "seat_index": 0, "alive": True},
                    {"participant_id": "casey", "display_name": "Casey", "is_human": False, "seat_index": 1, "alive": True},
                ],
            ),
            MafiaPrivateState(
                participant_id=config.agents[0].id,
                role=MafiaRole.TOWN,
                faction=MafiaFaction.TOWN,
                alive=True,
                can_vote=True,
            ),
            [],
        ),
    )
    assert "(human," not in prompt
    assert "(agent," not in prompt
    assert "- Alex (alive)" in prompt


def test_agent_input_json_strips_is_human_from_mafia_payloads(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    invoker = AgentInvoker(
        ScriptedAgentRuntime(),
        "run-test",
        Workspace(id="ws-test", path=ROOT),
    )
    prompt = invoker._compose_prompt(
        "vote prompt",
        PolicySet(config).mafia_vote_input(
            config.agents[0],
            MafiaPublicState(
                game_status=MafiaGameStatus.ACTIVE,
                phase=MafiaPhase.DAY_VOTE,
                roster=[
                    {"participant_id": "alex", "display_name": "Alex", "is_human": True, "seat_index": 0, "alive": True},
                ],
            ),
            MafiaPrivateState(
                participant_id=config.agents[0].id,
                role=MafiaRole.TOWN,
                faction=MafiaFaction.TOWN,
                alive=True,
                can_vote=True,
            ),
            [],
        ),
    )
    assert "\"is_human\"" not in prompt
    assert "\"display_name\": \"Alex\"" in prompt


def test_agent_input_json_collapses_public_sender_kind_to_participant(improved_config):
    config = improved_config.model_copy(deep=True)
    invoker = AgentInvoker(
        ScriptedAgentRuntime(),
        "run-test",
        Workspace(id="ws-test", path=ROOT),
    )
    now = utc_now()
    context = AgentContextSnapshot(
        agent_id=config.agents[0].id,
        watermark=3,
        current_time=now,
        recent_messages=[
            _message(
                message_id="m1",
                client_message_id="c1",
                sender_id="human-1",
                display_name="Daniel",
                text="hello there",
                created_at=now,
                sequence_no=1,
            ),
            _message(
                message_id="m2",
                client_message_id="c2",
                sender_id=config.agents[1].id,
                display_name=config.agents[1].display_name,
                sender_kind="agent",
                text="I think isolation matters.",
                created_at=now + timedelta(seconds=1),
                sequence_no=2,
            ),
            _message(
                message_id="m3",
                client_message_id="c3",
                sender_id="system",
                display_name="System",
                sender_kind="system",
                text="Day 1 begins.",
                created_at=now + timedelta(seconds=2),
                sequence_no=3,
            ),
        ],
        focus_messages=[],
        room_metrics=RoomMetricsSnapshot(active_participant_count=3),
        discourse_state=RoomDiscourseStateSnapshot(),
        active_participant_count=3,
        time_since_last_any=0.5,
        time_since_last_own=5.0,
        buffer_size=0,
        buffer_version=0,
        run_state="running",
    )
    prompt = invoker._compose_prompt(
        "generator prompt",
        PolicySet(config).generator_input(config.agents[0], context),
    )
    assert "\"sender_kind\": \"participant\"" in prompt
    assert "\"sender_kind\": \"system\"" in prompt
    assert "\"sender_kind\": \"human\"" not in prompt
    assert "\"sender_kind\": \"agent\"" not in prompt


def test_mafia_count_for_players_matches_original_ratio():
    assert mafia_count_for_players(5) == 2
    assert mafia_count_for_players(6) == 2
    assert mafia_count_for_players(7) == 2
    assert mafia_count_for_players(8) == 3
    assert mafia_count_for_players(9) == 3
    assert mafia_count_for_players(10) == 3
    assert mafia_count_for_players(11) == 4
    assert mafia_count_for_players(12) == 4
    assert mafia_count_for_players(13) == 4


def test_mafia_public_state_reveals_factions_only_after_game_over():
    active_snapshot = MafiaGameSnapshot(
        game_status=MafiaGameStatus.ACTIVE,
        phase=MafiaPhase.DAY_DISCUSSION,
        players=[
            MafiaPlayerRecord(participant_id="p1", display_name="Alex", is_human=True, seat_index=0, role=MafiaRole.MAFIA, faction=MafiaFaction.MAFIA),
            MafiaPlayerRecord(participant_id="p2", display_name="Jordan", is_human=True, seat_index=1, role=MafiaRole.TOWN, faction=MafiaFaction.TOWN),
        ],
    )
    active_public = active_snapshot.public_state()
    assert [entry.faction for entry in active_public.roster] == [None, None]

    game_over_snapshot = active_snapshot.model_copy(
        update={
            "game_status": MafiaGameStatus.GAME_OVER,
            "winner": MafiaFaction.MAFIA,
            "winning_participant_ids": ["p1"],
        }
    )
    game_over_public = game_over_snapshot.public_state()
    assert [entry.faction for entry in game_over_public.roster] == [MafiaFaction.MAFIA, MafiaFaction.TOWN]


@pytest.mark.asyncio
async def test_mafia_lobby_spinup_buffers_candidates_before_game_start(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    config.agents = [config.agents[0]]
    engine = ConversationEngine(config)
    await engine.start()
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == f"agent.event.{config.agents[0].id}.candidate.buffered",
        timeout=3.0,
    )
    events = await engine.export_events()
    assert engine.registry.buffer_for(config.agents[0].id)
    assert engine.registry.buffer_for(config.agents[0].id)[0].metadata.get("mafia_lobby_spinup") is True
    assert any(
        logged.event.subject == "debug.event.agent.workflow.completed"
        and logged.event.payload["agent_id"] == config.agents[0].id
        and (logged.event.payload.get("scheduler_decision") or {}).get("reason") == "mafia_lobby_spinup"
        for logged in events
    )
    assert not any(
        logged.event.subject == "debug.event.agent.call.started"
        and logged.event.payload["agent_id"] == config.agents[0].id
        and logged.event.payload["worker_kind"] == "scheduler"
        for logged in events
    )
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_mafia_day_discussion_phase_triggers_agent_workflow_without_waiting_for_chat(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    config.agents = [config.agents[0]]
    engine = ConversationEngine(config)
    await engine.start()
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == f"agent.event.{config.agents[0].id}.candidate.buffered",
        timeout=3.0,
    )
    started_at = utc_now()
    await engine.append_event_and_wait(
        make_event(
            "mafia.event.snapshot.updated",
            payload=MafiaGameSnapshot(
                game_status=MafiaGameStatus.ACTIVE,
                phase=MafiaPhase.DAY_DISCUSSION,
                phase_started_at=started_at,
                phase_ends_at=started_at + timedelta(seconds=45),
                total_players=config.mafia.total_players,
                round_no=1,
                players=[
                    MafiaPlayerRecord(
                        participant_id=config.agents[0].id,
                        display_name=config.agents[0].display_name,
                        is_human=False,
                        seat_index=0,
                        role=MafiaRole.TOWN,
                        faction=MafiaFaction.TOWN,
                        connected=True,
                    )
                ],
            ),
        )
    )
    scheduler_started = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.call.started"
        and logged.event.payload["agent_id"] == config.agents[0].id
        and logged.event.payload["worker_kind"] == "scheduler"
        and logged.event.payload["invocation"]["trigger_kind"] == "mafia_phase",
        timeout=3.0,
    )
    assert scheduler_started.event.payload["command_subject"] == f"agent.workflow.{config.agents[0].id}.schedule"
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_mafia_night_reveal_spinup_prebuffers_for_next_day(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    config.agents = [config.agents[0]]
    config.agents[0].generation.staleness_window_seconds = 0.05
    engine = ConversationEngine(config)
    await engine.start()
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == f"agent.event.{config.agents[0].id}.candidate.buffered",
        timeout=3.0,
    )
    night_started_at = utc_now()
    await engine.append_event_and_wait(
        make_event(
            "mafia.event.snapshot.updated",
            payload=MafiaGameSnapshot(
                game_status=MafiaGameStatus.ACTIVE,
                phase=MafiaPhase.NIGHT_REVEAL,
                phase_started_at=night_started_at,
                phase_ends_at=night_started_at + timedelta(seconds=45),
                total_players=config.mafia.total_players,
                round_no=1,
                players=[
                    MafiaPlayerRecord(
                        participant_id=config.agents[0].id,
                        display_name=config.agents[0].display_name,
                        is_human=False,
                        seat_index=0,
                        role=MafiaRole.TOWN,
                        faction=MafiaFaction.TOWN,
                        connected=True,
                    )
                ],
            ),
        )
    )
    pre_day_buffered = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == f"agent.event.{config.agents[0].id}.candidate.buffered"
        and logged.event.payload.get("metadata", {}).get("mafia_pre_day_spinup") is True,
        timeout=3.0,
    )
    day_started_at = utc_now()
    await engine.append_event_and_wait(
        make_event(
            "mafia.event.snapshot.updated",
            payload=MafiaGameSnapshot(
                game_status=MafiaGameStatus.ACTIVE,
                phase=MafiaPhase.DAY_DISCUSSION,
                phase_started_at=day_started_at,
                phase_ends_at=day_started_at + timedelta(seconds=45),
                total_players=config.mafia.total_players,
                round_no=2,
                players=[
                    MafiaPlayerRecord(
                        participant_id=config.agents[0].id,
                        display_name=config.agents[0].display_name,
                        is_human=False,
                        seat_index=0,
                        role=MafiaRole.TOWN,
                        faction=MafiaFaction.TOWN,
                        connected=True,
                    )
                ],
            ),
        )
    )
    scheduler_started = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.call.started"
        and logged.event.payload["agent_id"] == config.agents[0].id
        and logged.event.payload["worker_kind"] == "scheduler"
        and logged.event.payload["invocation"]["trigger_kind"] == "mafia_phase",
        timeout=3.0,
    )
    events = await engine.export_events()
    assert pre_day_buffered.event.payload["metadata"]["mafia_pre_day_spinup"] is True
    assert scheduler_started.event.payload["command_subject"] == f"agent.workflow.{config.agents[0].id}.schedule"
    assert not any(
        logged.event.subject == "debug.event.agent.call.started"
        and logged.event.payload["agent_id"] == config.agents[0].id
        and logged.event.payload["worker_kind"] == "generator"
        and logged.event.payload["invocation"]["trigger_kind"] == "mafia_phase"
        for logged in events
    )
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_mafia_scheduler_does_not_wait_for_generation_when_day_opens_with_empty_buffer(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    config.agents = [config.agents[0]]
    runtime = SequencedGeneratorRuntime([0.0, 0.35])
    engine = ConversationEngine(config, runtime=runtime)
    await engine.start()
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == f"agent.event.{config.agents[0].id}.candidate.buffered",
        timeout=1.5,
    )
    await _discard_agent_buffer(engine, config.agents[0].id)
    day_logged = await engine.append_event_and_wait(
        make_event(
            "mafia.event.snapshot.updated",
            payload=_mafia_day_discussion_snapshot(config.agents[0]),
        )
    )
    scheduler_wait = await _wait_for_event(
        engine,
        lambda logged: logged.seq > day_logged.seq
        and logged.event.subject == f"agent.event.{config.agents[0].id}.scheduler.decided"
        and logged.event.payload["reason"] == "no-buffered-candidate",
        timeout=1.0,
    )
    generator_completed = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.call.completed"
        and logged.event.payload["agent_id"] == config.agents[0].id
        and logged.event.payload["worker_kind"] == "generator"
        and logged.event.payload["invocation"]["trigger_kind"] == "mafia_phase",
        timeout=2.0,
    )
    assert scheduler_wait.seq < generator_completed.seq
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_mafia_generation_completion_retriggers_scheduler_with_buffer_ready(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    config.agents = [config.agents[0]]
    runtime = SequencedGeneratorRuntime([0.0, 0.2])
    engine = ConversationEngine(config, runtime=runtime)
    await engine.start()
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == f"agent.event.{config.agents[0].id}.candidate.buffered",
        timeout=1.5,
    )
    await _discard_agent_buffer(engine, config.agents[0].id)
    day_logged = await engine.append_event_and_wait(
        make_event(
            "mafia.event.snapshot.updated",
            payload=_mafia_day_discussion_snapshot(config.agents[0]),
        )
    )
    buffer_ready_started = await _wait_for_event(
        engine,
        lambda logged: logged.seq > day_logged.seq
        and logged.event.subject == "debug.event.agent.workflow.started"
        and logged.event.payload["agent_id"] == config.agents[0].id
        and logged.event.payload["trigger_kind"] == "buffer_ready",
        timeout=2.0,
    )
    scheduler_started = await _wait_for_event(
        engine,
        lambda logged: logged.seq > buffer_ready_started.seq
        and logged.event.subject == "debug.event.agent.call.started"
        and logged.event.payload["agent_id"] == config.agents[0].id
        and logged.event.payload["worker_kind"] == "scheduler"
        and logged.event.payload["invocation"]["trigger_kind"] == "buffer_ready",
        timeout=2.0,
    )
    assert scheduler_started.event.payload["command_subject"] == f"agent.workflow.{config.agents[0].id}.schedule"
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_mafia_buffer_ready_still_waits_for_scheduler_before_any_send(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    config.agents = [config.agents[0]]
    runtime = SequencedGeneratorRuntime([0.0, 0.2], wait_on_buffered_scheduler=True)
    engine = ConversationEngine(config, runtime=runtime)
    await engine.start()
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == f"agent.event.{config.agents[0].id}.candidate.buffered",
        timeout=1.5,
    )
    await _discard_agent_buffer(engine, config.agents[0].id)
    day_logged = await engine.append_event_and_wait(
        make_event(
            "mafia.event.snapshot.updated",
            payload=_mafia_day_discussion_snapshot(config.agents[0]),
        )
    )
    wait_decision = await _wait_for_event(
        engine,
        lambda logged: logged.seq > day_logged.seq
        and logged.event.subject == f"agent.event.{config.agents[0].id}.scheduler.decided"
        and logged.event.payload["reason"] == "buffered-not-relevant",
        timeout=2.0,
    )
    await asyncio.sleep(0.1)
    events = await engine.export_events()
    assert not any(
        logged.seq > wait_decision.seq
        and logged.event.subject == f"agent.event.{config.agents[0].id}.candidate.reserved"
        for logged in events
    )
    assert not any(
        logged.seq > wait_decision.seq
        and logged.event.subject == "conversation.event.message.committed"
        and logged.event.payload["sender_id"] == config.agents[0].id
        for logged in events
    )
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_mafia_inflight_generation_is_not_duplicated_by_room_messages(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    config.agents = [config.agents[0]]
    runtime = SequencedGeneratorRuntime([0.0, 0.5])
    engine = ConversationEngine(config, runtime=runtime)
    await engine.start()
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == f"agent.event.{config.agents[0].id}.candidate.buffered",
        timeout=1.5,
    )
    baseline_generator_calls = runtime.generator_calls
    await _discard_agent_buffer(engine, config.agents[0].id)
    await engine.append_event_and_wait(
        make_event(
            "mafia.event.snapshot.updated",
            payload=_mafia_day_discussion_snapshot(config.agents[0]),
        )
    )
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.call.started"
        and logged.event.payload["agent_id"] == config.agents[0].id
        and logged.event.payload["worker_kind"] == "generator"
        and logged.event.payload["invocation"]["trigger_kind"] == "mafia_phase",
        timeout=1.0,
    )
    await engine.submit_message(text="who feels suspicious so far?")
    await engine.submit_message(text="i want more concrete reads")
    await asyncio.sleep(0.15)
    assert runtime.generator_calls == baseline_generator_calls + 1
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_mafia_shutdown_cancels_inflight_background_generation(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    config.agents = [config.agents[0]]
    runtime = SequencedGeneratorRuntime([0.0, 5.0])
    engine = ConversationEngine(config, runtime=runtime)
    await engine.start()
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == f"agent.event.{config.agents[0].id}.candidate.buffered",
        timeout=1.5,
    )
    await _discard_agent_buffer(engine, config.agents[0].id)
    await engine.append_event_and_wait(
        make_event(
            "mafia.event.snapshot.updated",
            payload=_mafia_day_discussion_snapshot(config.agents[0]),
        )
    )
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.call.started"
        and logged.event.payload["agent_id"] == config.agents[0].id
        and logged.event.payload["worker_kind"] == "generator"
        and logged.event.payload["invocation"]["trigger_kind"] == "mafia_phase",
        timeout=1.0,
    )
    await asyncio.wait_for(_shutdown(engine), timeout=1.0)


@pytest.mark.asyncio
async def test_slow_mafia_voters_do_not_block_phase_advance(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    config.mode = ModeProfile.IMPROVED_BUFFERED_ASYNC
    config.topic.enabled = False
    config.chat.max_duration_seconds = None
    config.chat.max_messages = None
    config.mafia.total_players = 5
    config.mafia.day_discussion_seconds = 0.05
    config.mafia.day_vote_seconds = 0.05
    config.mafia.day_reveal_seconds = 0.05
    config.mafia.night_action_seconds = 1.0
    config.mafia.night_reveal_seconds = 0.05
    config.agents = generate_mafia_personas("phase-advance-timing", config.mafia.total_players)

    engine = ConversationEngine(config, runtime=SlowVoterRuntime(delay_seconds=0.25))
    await engine.start()
    await engine.dispatch_command(
        CommandEnvelope(
            subject="mafia.command.game.start",
            payload={"humans": []},
        )
    )

    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "mafia.event.snapshot.updated"
        and logged.event.payload["phase"] == MafiaPhase.DAY_VOTE.value,
        timeout=1.0,
    )
    started = asyncio.get_running_loop().time()
    revealed = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "mafia.event.vote.revealed"
        and logged.event.payload["phase"] == MafiaPhase.DAY_VOTE.value,
        timeout=0.35,
    )
    elapsed = asyncio.get_running_loop().time() - started

    assert revealed.event.payload["summary"]
    assert elapsed < 0.35
    await _shutdown(engine)


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


def test_mafia_scheduler_input_allows_more_talk_before_switching_to_listening(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    policies = PolicySet(config)
    agent = config.agents[0]
    current_time = __import__("datetime").datetime.now(__import__("datetime").UTC)
    view = AgentContextSnapshot(
        agent_id=agent.id,
        watermark=0,
        current_time=current_time,
        recent_messages=[],
        focus_messages=[],
        room_metrics=RoomMetricsSnapshot(active_participant_count=4, avg_message_rate=1 / 4),
        active_participant_count=4,
        agent_message_rate=0.35,
        avg_message_rate=1 / 4,
        time_since_last_any=0.0,
        time_since_last_own=0.0,
        buffer_size=0,
        buffer_version=0,
        run_state="running",
    )
    scheduler_input = policies.scheduler_input(agent, view)
    assert scheduler_input.talk_mode == "talkative"


@pytest.mark.asyncio
async def test_improved_mode_uses_transport_and_shared_conversation_boundary(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.chat.typing_words_per_second = 100.0
    engine = ConversationEngine(config)
    await engine.start()
    ack = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "transport.event.message.acked",
        timeout=3.0,
    )
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
    first_reserved = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject.endswith(".candidate.reserved"),
        timeout=3.0,
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
    recent_message = _message(
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


def test_scheduler_input_prefers_direct_mention_for_reply_target(improved_config):
    policies = PolicySet(improved_config)
    agent = improved_config.agents[0]
    current_time = utc_now()
    older_question = _message(
        message_id="m1",
        client_message_id="c1",
        sender_id="jordan",
        display_name="Jordan",
        text="what do you think?",
        created_at=current_time - timedelta(seconds=2),
        sequence_no=1,
    )
    direct_mention = _message(
        message_id="m2",
        client_message_id="c2",
        sender_id="casey",
        display_name="Casey",
        text=f"{agent.display_name}, that's a bad read",
        created_at=current_time - timedelta(seconds=1),
        sequence_no=2,
        mentions=[agent.id],
    )
    context = AgentContextSnapshot(
        agent_id=agent.id,
        watermark=2,
        current_time=current_time,
        recent_messages=[older_question, direct_mention],
        focus_messages=[older_question, direct_mention],
        focus_message_ids=["m1", "m2"],
        room_metrics=RoomMetricsSnapshot(watermark=2, active_participant_count=3, recent_keyword_sketch=["read", "vote"]),
        active_participant_count=3,
        time_since_last_any=0.8,
        time_since_last_own=4.0,
        buffer_size=1,
        buffer_version=1,
        run_state="running",
    )
    candidate = CandidateRecord(candidate_id="cand-1", agent_id=agent.id, text="nah that doesn't track", created_at=current_time)
    scheduler_input = policies.scheduler_input(agent, context, buffer_candidates=[candidate], active_reservations=[])
    assert scheduler_input.reply_target_message_id == "m2"
    assert scheduler_input.reply_target_reason == "direct_mention"
    assert scheduler_input.obligation_strength == "high"
    assert scheduler_input.floor_state == "addressed_response_slot"


def test_scheduler_input_prefers_reply_hint_over_recent_turn(improved_config):
    policies = PolicySet(improved_config)
    agent = improved_config.agents[0]
    current_time = utc_now()
    hinted = _message(
        message_id="m1",
        client_message_id="c1",
        sender_id="jordan",
        display_name="Jordan",
        text="responding to you here",
        created_at=current_time - timedelta(seconds=2),
        sequence_no=1,
        reply_hint=agent.id,
    )
    recent_turn = _message(
        message_id="m2",
        client_message_id="c2",
        sender_id="casey",
        display_name="Casey",
        text="another thought",
        created_at=current_time - timedelta(seconds=1),
        sequence_no=2,
    )
    context = AgentContextSnapshot(
        agent_id=agent.id,
        watermark=2,
        current_time=current_time,
        recent_messages=[hinted, recent_turn],
        focus_messages=[hinted, recent_turn],
        focus_message_ids=["m1", "m2"],
        room_metrics=RoomMetricsSnapshot(watermark=2, active_participant_count=3),
        active_participant_count=3,
        time_since_last_any=0.5,
        time_since_last_own=3.0,
        buffer_size=1,
        buffer_version=1,
        run_state="running",
    )
    candidate = CandidateRecord(candidate_id="cand-1", agent_id=agent.id, text="here's my read", created_at=current_time)
    scheduler_input = policies.scheduler_input(agent, context, buffer_candidates=[candidate], active_reservations=[])
    assert scheduler_input.reply_target_message_id == "m1"
    assert scheduler_input.reply_target_reason == "reply_hint"


def test_scheduler_input_direct_question_yields_high_obligation(improved_config):
    policies = PolicySet(improved_config)
    agent = improved_config.agents[0]
    current_time = utc_now()
    question = _message(
        message_id="m1",
        client_message_id="c1",
        sender_id="jordan",
        display_name="Jordan",
        text=f"{agent.display_name}, who are you voting?",
        created_at=current_time - timedelta(seconds=1),
        sequence_no=1,
        mentions=[agent.id],
    )
    context = AgentContextSnapshot(
        agent_id=agent.id,
        watermark=1,
        current_time=current_time,
        recent_messages=[question],
        focus_messages=[question],
        focus_message_ids=["m1"],
        room_metrics=RoomMetricsSnapshot(watermark=1, active_participant_count=2),
        active_participant_count=2,
        time_since_last_any=0.2,
        time_since_last_own=5.0,
        buffer_size=1,
        buffer_version=1,
        run_state="running",
    )
    candidate = CandidateRecord(candidate_id="cand-1", agent_id=agent.id, text="i'm leaning jordan", created_at=current_time)
    scheduler_input = policies.scheduler_input(agent, context, buffer_candidates=[candidate], active_reservations=[])
    assert scheduler_input.obligation_strength == "high"
    assert scheduler_input.candidate_turn_kind == "answer"


def test_scheduler_input_recent_self_turn_yields_cooldown_floor_state(improved_config):
    policies = PolicySet(improved_config)
    agent = improved_config.agents[0]
    current_time = utc_now()
    prior_other = _message(
        message_id="m1",
        client_message_id="c1",
        sender_id="casey",
        display_name="Casey",
        text="maybe we split votes",
        created_at=current_time - timedelta(seconds=1),
        sequence_no=1,
    )
    own_recent = _message(
        message_id="m2",
        client_message_id="c2",
        sender_id=agent.id,
        display_name=agent.display_name,
        text="i still think casey's off",
        created_at=current_time - timedelta(seconds=0.1),
        sequence_no=2,
    )
    context = AgentContextSnapshot(
        agent_id=agent.id,
        watermark=2,
        current_time=current_time,
        recent_messages=[prior_other, own_recent],
        focus_messages=[prior_other, own_recent],
        focus_message_ids=["m1", "m2"],
        room_metrics=RoomMetricsSnapshot(watermark=2, active_participant_count=3),
        active_participant_count=3,
        time_since_last_any=0.1,
        time_since_last_own=0.1,
        has_sent_message=True,
        own_message_count=1,
        buffer_size=1,
        buffer_version=1,
        run_state="running",
    )
    candidate = CandidateRecord(candidate_id="cand-1", agent_id=agent.id, text="and jordan too", created_at=current_time)
    scheduler_input = policies.scheduler_input(agent, context, buffer_candidates=[candidate], active_reservations=[])
    assert scheduler_input.obligation_strength in {"low", "none"}
    assert scheduler_input.floor_state == "cooldown_after_self_turn"


def test_mafia_active_burst_yields_brief_overlap_ok_floor_state(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    policies = PolicySet(config)
    agent = config.agents[0]
    current_time = utc_now()
    recent = [
        _message(
            message_id=f"m{index}",
            client_message_id=f"c{index}",
            sender_id=f"other-{index}",
            display_name=f"Other {index}",
            text=f"message {index}",
            created_at=current_time - timedelta(seconds=1.5 - (index * 0.2)),
            sequence_no=index,
        )
        for index in range(1, 4)
    ]
    context = AgentContextSnapshot(
        agent_id=agent.id,
        watermark=3,
        current_time=current_time,
        recent_messages=recent,
        focus_messages=recent,
        focus_message_ids=[message.message_id for message in recent],
        room_metrics=RoomMetricsSnapshot(watermark=3, active_participant_count=4),
        active_participant_count=4,
        time_since_last_any=0.4,
        time_since_last_own=4.0,
        recent_message_count=3,
        buffer_size=1,
        buffer_version=1,
        run_state="running",
    )
    candidate = CandidateRecord(candidate_id="cand-1", agent_id=agent.id, text="yeah exactly", created_at=current_time)
    scheduler_input = policies.scheduler_input(
        agent,
        context,
        buffer_candidates=[candidate],
        active_reservations=[],
        mafia_public_state=MafiaPublicState(game_status=MafiaGameStatus.ACTIVE, phase=MafiaPhase.DAY_DISCUSSION),
        mafia_private_state=MafiaPrivateState(participant_id=agent.id, role=MafiaRole.TOWN, faction=MafiaFaction.TOWN, alive=True),
    )
    assert scheduler_input.floor_state == "brief_overlap_ok"
    assert scheduler_input.candidate_turn_kind == "backchannel"


def test_scripted_scheduler_allows_same_text_when_reply_target_differs():
    logic = ScriptedAgentLogic()
    current_time = utc_now()
    context = AgentContextSnapshot(
        agent_id="alex",
        watermark=2,
        current_time=current_time,
        recent_messages=[],
        focus_messages=[],
        room_metrics=RoomMetricsSnapshot(active_participant_count=3),
        active_participant_count=3,
        time_since_last_any=3.0,
        time_since_last_own=5.0,
        buffer_size=1,
        buffer_version=1,
        run_state="running",
    )
    snapshot = SchedulerInputSnapshot(
        scenario="Mafia",
        agent_context=context,
        goals=[],
        talk_mode="talkative",
        current_time_label=current_time.isoformat(),
        has_buffered_candidate=True,
        candidate_preview_text="yeah same",
        candidate_similarity_score=0.99,
        similar_recent_message_text="yeah same",
        similar_recent_message_age_seconds=1.0,
        similar_recent_same_reply_target=False,
        similar_recent_same_turn_kind=True,
        reply_target_display_name="Jordan",
        reply_target_reason="recent_question",
        obligation_strength="medium",
        floor_state="open_floor",
        candidate_turn_kind="agreement",
    )
    reply = logic.scheduler_reply(snapshot, {"talkativeness": 0.9, "confidence": 0.9, "reactivity": 1.0})
    assert reply.decision == "send"


def test_scripted_scheduler_waits_on_exact_echo_same_target_same_turn():
    logic = ScriptedAgentLogic()
    current_time = utc_now()
    context = AgentContextSnapshot(
        agent_id="alex",
        watermark=2,
        current_time=current_time,
        recent_messages=[],
        focus_messages=[],
        room_metrics=RoomMetricsSnapshot(active_participant_count=3),
        active_participant_count=3,
        time_since_last_any=3.0,
        time_since_last_own=5.0,
        buffer_size=1,
        buffer_version=1,
        run_state="running",
    )
    snapshot = SchedulerInputSnapshot(
        scenario="Mafia",
        agent_context=context,
        goals=[],
        talk_mode="talkative",
        current_time_label=current_time.isoformat(),
        has_buffered_candidate=True,
        candidate_preview_text="yeah same",
        candidate_similarity_score=0.99,
        similar_recent_message_text="yeah same",
        similar_recent_message_age_seconds=1.0,
        similar_recent_same_reply_target=True,
        similar_recent_same_turn_kind=True,
        reply_target_display_name="Jordan",
        reply_target_reason="recent_question",
        obligation_strength="medium",
        floor_state="open_floor",
        candidate_turn_kind="agreement",
    )
    reply = logic.scheduler_reply(snapshot, {"talkativeness": 0.9, "confidence": 0.9, "reactivity": 1.0})
    assert reply.decision == "wait"
    assert reply.reason == "duplicate-recent-message"


def test_scripted_scheduler_sends_direct_question_even_when_room_is_active():
    logic = ScriptedAgentLogic()
    current_time = utc_now()
    context = AgentContextSnapshot(
        agent_id="alex",
        watermark=2,
        current_time=current_time,
        recent_messages=[],
        focus_messages=[],
        room_metrics=RoomMetricsSnapshot(active_participant_count=3),
        active_participant_count=3,
        time_since_last_any=0.01,
        time_since_last_own=5.0,
        buffer_size=1,
        buffer_version=1,
        run_state="running",
    )
    snapshot = SchedulerInputSnapshot(
        scenario="Mafia",
        agent_context=context,
        goals=[],
        talk_mode="listening",
        current_time_label=current_time.isoformat(),
        has_buffered_candidate=True,
        candidate_preview_text="i'm voting jordan",
        reply_target_display_name="Jordan",
        reply_target_reason="direct_mention",
        obligation_strength="high",
        floor_state="addressed_response_slot",
        candidate_turn_kind="answer",
    )
    reply = logic.scheduler_reply(snapshot, {"talkativeness": 0.8, "confidence": 0.8, "reactivity": 1.0})
    assert reply.decision == "send"


def test_scripted_scheduler_allows_brief_acks_more_readily_in_mafia_overlap_window():
    logic = ScriptedAgentLogic()
    current_time = utc_now()
    context = AgentContextSnapshot(
        agent_id="alex",
        watermark=2,
        current_time=current_time,
        recent_messages=[],
        focus_messages=[],
        room_metrics=RoomMetricsSnapshot(active_participant_count=5),
        active_participant_count=5,
        time_since_last_any=0.08,
        time_since_last_own=5.0,
        buffer_size=1,
        buffer_version=1,
        run_state="running",
    )
    regular_snapshot = SchedulerInputSnapshot(
        scenario="Regular room",
        agent_context=context,
        goals=[],
        talk_mode="talkative",
        current_time_label=current_time.isoformat(),
        has_buffered_candidate=True,
        candidate_preview_text="yeah exactly",
        reply_target_display_name="Jordan",
        reply_target_reason="recent_turn",
        obligation_strength="low",
        floor_state="open_floor",
        candidate_turn_kind="backchannel",
    )
    mafia_snapshot = regular_snapshot.model_copy(update={"scenario": "Mafia", "floor_state": "brief_overlap_ok"})
    regular_reply = logic.scheduler_reply(regular_snapshot, {"talkativeness": 0.55, "confidence": 0.45, "reactivity": 1.0})
    mafia_reply = logic.scheduler_reply(mafia_snapshot, {"talkativeness": 0.55, "confidence": 0.45, "reactivity": 1.0})
    assert regular_reply.decision == "wait"
    assert mafia_reply.decision == "send"


@pytest.mark.asyncio
async def test_discourse_projection_direct_question_assigns_and_clears_slot(improved_config):
    event_log = EventLog()
    registry = ProjectionRegistry(event_log, improved_config)
    await registry.start()
    agent = improved_config.agents[0]
    now = utc_now()

    question = _message(
        message_id="m1",
        client_message_id="c1",
        sender_id="human-1",
        display_name="Daniel",
        text=f"{agent.display_name}, what do you mean by that?",
        created_at=now,
        sequence_no=1,
        mentions=[agent.id],
    )
    logged = await event_log.append(
        make_event(
            "conversation.event.message.committed",
            payload=question.model_dump(mode="json"),
        )
    )
    await registry.wait_until(logged.seq)
    state = registry.discourse_state()
    assert state.strict_turn_active is True
    assert state.slot_owner_id == agent.id
    assert state.slot_reason == "direct_question"
    assert state.open_questions[0].target_participant_id == agent.id

    answer = _message(
        message_id="m2",
        client_message_id="c2",
        sender_id=agent.id,
        display_name=agent.display_name,
        sender_kind="agent",
        text="I mean each project should stay isolated.",
        created_at=now + timedelta(seconds=1),
        sequence_no=2,
    )
    logged = await event_log.append(
        make_event(
            "conversation.event.message.committed",
            payload=answer.model_dump(mode="json"),
        )
    )
    await registry.wait_until(logged.seq)
    state = registry.discourse_state()
    assert state.strict_turn_active is False
    assert state.slot_owner_id is None
    await registry.close()


@pytest.mark.asyncio
async def test_mafia_discourse_projection_does_not_assign_strict_turn_for_direct_question(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    event_log = EventLog()
    registry = ProjectionRegistry(event_log, config)
    await registry.start()
    agent = config.agents[0]
    now = utc_now()

    question = _message(
        message_id="m1",
        client_message_id="c1",
        sender_id="human-1",
        display_name="Daniel",
        text=f"{agent.display_name}, what do you mean by that?",
        created_at=now,
        sequence_no=1,
        mentions=[agent.id],
    )
    logged = await event_log.append(
        make_event(
            "conversation.event.message.committed",
            payload=question.model_dump(mode="json"),
        )
    )
    await registry.wait_until(logged.seq)

    state = registry.discourse_state()
    assert state.strict_turn_active is False
    assert state.slot_owner_id is None
    assert state.slot_reason == "none"
    assert state.open_questions[0].target_participant_id is None
    await registry.close()


@pytest.mark.asyncio
async def test_discourse_projection_tracks_agent_questions_source_agnostically(improved_config):
    event_log = EventLog()
    registry = ProjectionRegistry(event_log, improved_config)
    await registry.start()
    now = utc_now()
    asker = improved_config.agents[0]
    target = improved_config.agents[1]

    question = _message(
        message_id="m1",
        client_message_id="c1",
        sender_id=asker.id,
        display_name=asker.display_name,
        sender_kind="agent",
        text=f"{target.display_name}, what do you mean by shared pain?",
        created_at=now,
        sequence_no=1,
        mentions=[target.id],
    )
    logged = await event_log.append(
        make_event(
            "conversation.event.message.committed",
            payload=question.model_dump(mode="json"),
        )
    )
    await registry.wait_until(logged.seq)
    state = registry.discourse_state()
    assert state.strict_turn_active is True
    assert state.slot_owner_id == target.id
    assert state.open_questions[0].asker_id == asker.id
    assert state.open_questions[0].target_participant_id == target.id

    answer = _message(
        message_id="m2",
        client_message_id="c2",
        sender_id=target.id,
        display_name=target.display_name,
        sender_kind="agent",
        text="I mean we all share the operational costs.",
        created_at=now + timedelta(seconds=1),
        sequence_no=2,
    )
    logged = await event_log.append(
        make_event(
            "conversation.event.message.committed",
            payload=answer.model_dump(mode="json"),
        )
    )
    await registry.wait_until(logged.seq)
    state = registry.discourse_state()
    assert state.strict_turn_active is False
    assert state.slot_owner_id is None
    assert "m1" in state.resolved_question_ids
    await registry.close()


@pytest.mark.asyncio
async def test_discourse_projection_unnamed_strict_turn_uses_round_robin(improved_config):
    event_log = EventLog()
    registry = ProjectionRegistry(event_log, improved_config)
    await registry.start()
    now = utc_now()

    first_prompt = _message(
        message_id="m1",
        client_message_id="c1",
        sender_id="human-1",
        display_name="Daniel",
        text="One at a time please.",
        created_at=now,
        sequence_no=1,
    )
    logged = await event_log.append(
        make_event(
            "conversation.event.message.committed",
            payload=first_prompt.model_dump(mode="json"),
        )
    )
    await registry.wait_until(logged.seq)
    first_state = registry.discourse_state()
    assert first_state.slot_owner_id == improved_config.agents[0].id
    assert first_state.slot_reason == "strict_turn_unnamed"

    first_reply = _message(
        message_id="m2",
        client_message_id="c2",
        sender_id=improved_config.agents[0].id,
        display_name=improved_config.agents[0].display_name,
        sender_kind="agent",
        text="State isolation is the key constraint.",
        created_at=now + timedelta(seconds=1),
        sequence_no=2,
    )
    logged = await event_log.append(
        make_event(
            "conversation.event.message.committed",
            payload=first_reply.model_dump(mode="json"),
        )
    )
    await registry.wait_until(logged.seq)

    second_prompt = _message(
        message_id="m3",
        client_message_id="c3",
        sender_id="human-1",
        display_name="Daniel",
        text="One at a time please.",
        created_at=now + timedelta(seconds=2),
        sequence_no=3,
    )
    logged = await event_log.append(
        make_event(
            "conversation.event.message.committed",
            payload=second_prompt.model_dump(mode="json"),
        )
    )
    await registry.wait_until(logged.seq)
    second_state = registry.discourse_state()
    assert second_state.slot_owner_id == improved_config.agents[1].id
    assert second_state.last_strict_turn_owner_id == improved_config.agents[1].id
    await registry.close()


@pytest.mark.asyncio
async def test_discourse_projection_tracks_resolved_questions_and_commitments(improved_config):
    event_log = EventLog()
    registry = ProjectionRegistry(event_log, improved_config)
    await registry.start()
    now = utc_now()

    messages = [
        _message(
            message_id="m1",
            client_message_id="c1",
            sender_id="human-1",
            display_name="Daniel",
            text="Should we isolate state per project?",
            created_at=now,
            sequence_no=1,
        ),
        _message(
            message_id="m2",
            client_message_id="c2",
            sender_id="human-1",
            display_name="Daniel",
            text="Yes, state isolation should be locked per project.",
            created_at=now + timedelta(seconds=1),
            sequence_no=2,
        ),
        _message(
            message_id="m3",
            client_message_id="c3",
            sender_id="human-1",
            display_name="Daniel",
            text="No context switching between projects.",
            created_at=now + timedelta(seconds=2),
            sequence_no=3,
        ),
    ]
    for message in messages:
        logged = await event_log.append(
            make_event(
                "conversation.event.message.committed",
                payload=message.model_dump(mode="json"),
            )
        )
        await registry.wait_until(logged.seq)

    state = registry.discourse_state()
    assert not state.open_questions
    assert "m1" in state.resolved_question_ids
    assert any(commitment.source_message_id == "m2" for commitment in state.accepted_commitments)
    assert any(commitment.source_message_id == "m3" for commitment in state.rejected_commitments)
    await registry.close()


@pytest.mark.asyncio
async def test_discourse_projection_tracks_agent_proposals_without_ratifying_when_humans_authoritative(improved_config):
    event_log = EventLog()
    registry = ProjectionRegistry(event_log, improved_config)
    await registry.start()
    now = utc_now()
    agent = improved_config.agents[0]

    proposal = _message(
        message_id="m1",
        client_message_id="c1",
        sender_id=agent.id,
        display_name=agent.display_name,
        sender_kind="agent",
        text="State isolation should be locked per project.",
        created_at=now,
        sequence_no=1,
    )
    logged = await event_log.append(
        make_event(
            "conversation.event.message.committed",
            payload=proposal.model_dump(mode="json"),
        )
    )
    await registry.wait_until(logged.seq)

    state = registry.discourse_state()
    assert any(item.source_message_id == "m1" for item in state.recent_proposals)
    assert not state.accepted_commitments
    assert not state.rejected_commitments
    await registry.close()


@pytest.mark.asyncio
async def test_discourse_projection_ratifies_agent_claims_in_mafia_mode_even_when_humans_authoritative(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    event_log = EventLog()
    registry = ProjectionRegistry(event_log, config)
    await registry.start()
    now = utc_now()
    agent = config.agents[0]

    claim = _message(
        message_id="m1",
        client_message_id="c1",
        sender_id=agent.id,
        display_name=agent.display_name,
        sender_kind="agent",
        text="State isolation should be locked per project.",
        created_at=now,
        sequence_no=1,
    )
    logged = await event_log.append(
        make_event(
            "conversation.event.message.committed",
            payload=claim.model_dump(mode="json"),
        )
    )
    await registry.wait_until(logged.seq)

    state = registry.discourse_state()
    assert any(commitment.source_message_id == "m1" for commitment in state.accepted_commitments)
    assert not state.recent_proposals
    await registry.close()


@pytest.mark.asyncio
async def test_discourse_projection_can_ratify_agent_claims_when_human_authority_is_disabled(improved_config):
    config = improved_config.model_copy(deep=True)
    config.authority.human_users_authoritative = False
    event_log = EventLog()
    registry = ProjectionRegistry(event_log, config)
    await registry.start()
    now = utc_now()
    agent = config.agents[0]

    claim = _message(
        message_id="m1",
        client_message_id="c1",
        sender_id=agent.id,
        display_name=agent.display_name,
        sender_kind="agent",
        text="State isolation should be locked per project.",
        created_at=now,
        sequence_no=1,
    )
    logged = await event_log.append(
        make_event(
            "conversation.event.message.committed",
            payload=claim.model_dump(mode="json"),
        )
    )
    await registry.wait_until(logged.seq)

    state = registry.discourse_state()
    assert any(commitment.source_message_id == "m1" for commitment in state.accepted_commitments)
    assert not state.recent_proposals
    await registry.close()


def test_scheduler_input_flags_reopening_resolved_question_and_commitment_conflict(improved_config):
    policies = PolicySet(improved_config)
    agent = improved_config.agents[0]
    current_time = utc_now()
    discourse_state = RoomDiscourseStateSnapshot(
        resolved_question_ids=["m1"],
        resolved_questions=[
            OpenQuestionState(
                question_id="m1",
                source_message_id="m1",
                asker_id="human-1",
                asker_display_name="Daniel",
                text_excerpt="Should we isolate state per project?",
                keyword_sketch=["isolate", "state", "project"],
                created_at=current_time - timedelta(seconds=5),
                resolved=True,
                resolved_by_message_id="m2",
            )
        ],
        rejected_commitments=[
            CommitmentState(
                source_message_id="m3",
                polarity="rejected",
                keyword_sketch=["context", "switching", "project"],
                canonical_text="No context switching between projects.",
                created_at=current_time - timedelta(seconds=4),
            )
        ],
    )
    context = AgentContextSnapshot(
        agent_id=agent.id,
        watermark=3,
        current_time=current_time,
        recent_messages=[],
        focus_messages=[],
        room_metrics=RoomMetricsSnapshot(watermark=3, active_participant_count=3),
        discourse_state=discourse_state,
        active_participant_count=3,
        time_since_last_any=0.8,
        time_since_last_own=4.0,
        buffer_size=1,
        buffer_version=1,
        run_state="running",
    )
    candidate = CandidateRecord(
        candidate_id="cand-1",
        agent_id=agent.id,
        text="Should we allow context switching between projects instead of isolating state per project?",
        created_at=current_time,
    )
    scheduler_input = policies.scheduler_input(agent, context, buffer_candidates=[candidate], active_reservations=[])
    assert scheduler_input.candidate_reopens_resolved_question is True
    assert scheduler_input.candidate_conflicts_with_commitment is True


def test_scripted_scheduler_waits_when_strict_turn_belongs_to_someone_else():
    logic = ScriptedAgentLogic()
    current_time = utc_now()
    context = AgentContextSnapshot(
        agent_id="alex",
        watermark=2,
        current_time=current_time,
        recent_messages=[],
        focus_messages=[],
        room_metrics=RoomMetricsSnapshot(active_participant_count=3),
        active_participant_count=3,
        time_since_last_any=0.2,
        time_since_last_own=5.0,
        buffer_size=1,
        buffer_version=1,
        run_state="running",
    )
    snapshot = SchedulerInputSnapshot(
        scenario="Regular room",
        agent_context=context,
        goals=[],
        talk_mode="talkative",
        current_time_label=current_time.isoformat(),
        has_buffered_candidate=True,
        candidate_preview_text="here's my take",
        strict_turn_active=True,
        slot_owner_id="jordan",
        slot_reason="direct_question",
        candidate_matches_slot=False,
        obligation_strength="none",
        floor_state="open_floor",
        candidate_turn_kind="answer",
    )
    reply = logic.scheduler_reply(snapshot, {"talkativeness": 1.0, "confidence": 1.0, "reactivity": 1.0})
    assert reply.decision == "wait"
    assert reply.reason == "strict_turn_slot_taken"


def test_mafia_policy_ignores_strict_turn_state(improved_config):
    config = improved_config.model_copy(deep=True)
    config.room_mode = RoomMode.MAFIA
    policies = PolicySet(config)
    agent = config.agents[0]
    current_time = utc_now()
    context = AgentContextSnapshot(
        agent_id=agent.id,
        watermark=2,
        current_time=current_time,
        recent_messages=[],
        focus_messages=[],
        room_metrics=RoomMetricsSnapshot(active_participant_count=3),
        discourse_state=RoomDiscourseStateSnapshot(
            strict_turn_active=True,
            slot_owner_id="jordan",
            slot_reason="direct_question",
        ),
        active_participant_count=3,
        time_since_last_any=0.2,
        time_since_last_own=5.0,
        buffer_size=1,
        buffer_version=1,
        run_state="running",
    )
    candidate = CandidateRecord(
        candidate_id="cand-1",
        agent_id=agent.id,
        text="here's my take",
        created_at=current_time,
    )

    assert policies.discourse_guard_reason(agent, context, candidate) is None

    scheduler_input = policies.scheduler_input(agent, context, buffer_candidates=[candidate], active_reservations=[])
    assert scheduler_input.strict_turn_active is False
    assert scheduler_input.slot_owner_id is None
    assert scheduler_input.slot_reason == "none"

    generator_input = policies.generator_input(agent, context)
    assert generator_input.owns_response_slot is True


def test_generator_prompt_includes_commitments_and_contribution_mode(improved_config):
    config = improved_config.model_copy(deep=True)
    agent = config.agents[0]
    agent.goals = ["define the system architecture and technical spec"]
    policies = PolicySet(config)
    current_time = utc_now()
    discourse_state = RoomDiscourseStateSnapshot(
        strict_turn_active=True,
        slot_owner_id=agent.id,
        slot_reason="direct_question",
        open_questions=[
            OpenQuestionState(
                question_id="m1",
                source_message_id="m1",
                asker_id="human-1",
                asker_display_name="Daniel",
                text_excerpt="How should isolation work per project?",
                keyword_sketch=["isolation", "project"],
                created_at=current_time - timedelta(seconds=4),
            )
        ],
        recent_proposals=[
            ProposalState(
                source_message_id="m0",
                proposer_id="jordan",
                proposer_display_name="Jordan",
                keyword_sketch=["memo", "friday"],
                canonical_text="We could send a Friday memo first.",
                created_at=current_time - timedelta(seconds=5),
            )
        ],
        accepted_commitments=[
            CommitmentState(
                source_message_id="m2",
                polarity="accepted",
                keyword_sketch=["isolation", "project"],
                canonical_text="State isolation should be locked per project.",
                created_at=current_time - timedelta(seconds=3),
            )
        ],
        rejected_commitments=[
            CommitmentState(
                source_message_id="m3",
                polarity="rejected",
                keyword_sketch=["context", "switching"],
                canonical_text="No context switching between projects.",
                created_at=current_time - timedelta(seconds=2),
            )
        ],
    )
    context = AgentContextSnapshot(
        agent_id=agent.id,
        watermark=3,
        current_time=current_time,
        recent_messages=[],
        focus_messages=[],
        room_metrics=RoomMetricsSnapshot(watermark=3, active_participant_count=3),
        discourse_state=discourse_state,
        active_participant_count=3,
        time_since_last_any=0.5,
        time_since_last_own=3.0,
        buffer_size=0,
        buffer_version=0,
        run_state="running",
    )
    prompt = policies.prompts.generator_prompt(agent, policies.generator_input(agent, context))
    assert "Contribution mode: concretize_constraints" in prompt
    assert "Recent participant proposals (tentative context only):" in prompt
    assert "Jordan: We could send a Friday memo first." in prompt
    assert "Recent accepted decisions:" in prompt
    assert "State isolation should be locked per project." in prompt
    assert "Recent rejected decisions:" in prompt
    assert "No context switching between projects." in prompt
    assert "Unresolved open questions:" in prompt
    assert "How should isolation work per project?" in prompt
    assert "You currently own the response slot: True" in prompt


@pytest.mark.asyncio
async def test_delivery_worker_aborts_candidate_that_reopens_resolved_question(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    agent = config.agents[0]
    event_log = EventLog()
    registry = ProjectionRegistry(event_log, config)
    await registry.start()
    engine = DeliveryTestEngine(config, registry, event_log)
    worker = AgentDeliveryWorker(engine, config, agent)
    now = utc_now()

    history = [
        _message(
            message_id="m1",
            client_message_id="c1",
            sender_id="human-1",
            display_name="Daniel",
            text="Should we isolate state per project?",
            created_at=now,
            sequence_no=1,
        ),
        _message(
            message_id="m2",
            client_message_id="c2",
            sender_id="human-1",
            display_name="Daniel",
            text="Yes, state isolation should be locked per project.",
            created_at=now + timedelta(seconds=1),
            sequence_no=2,
        ),
    ]
    for message in history:
        logged = await event_log.append(
            make_event(
                "conversation.event.message.committed",
                payload=message.model_dump(mode="json"),
            )
        )
        await registry.wait_until(logged.seq)

    reservation = DeliveryReservation(
        reservation_id="res-1",
        agent_id=agent.id,
        candidate=CandidateRecord(
            candidate_id="cand-1",
            agent_id=agent.id,
            text="Should we revisit project isolation?",
            created_at=utc_now(),
        ),
        client_message_id="client-1",
        created_at=utc_now(),
    )
    logged = await event_log.append(
        make_event(
            f"agent.event.{agent.id}.candidate.reserved",
            payload=reservation.model_dump(mode="json"),
        )
    )
    await registry.wait_until(logged.seq)

    command = CommandEnvelope(
        subject=f"agent.command.{agent.id}.deliver.request",
        payload={"reservation_id": reservation.reservation_id},
    )
    await worker.handle_request(command)

    events = await event_log.snapshot()
    aborted = next(
        logged
        for logged in events
        if logged.event.subject == f"agent.event.{agent.id}.delivery.aborted"
    )
    assert aborted.event.payload["reason"] == "resolved_question_reopened"
    assert not engine.commands
    await registry.close()


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
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject.endswith(".candidate.reserved"),
        timeout=3.0,
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
async def test_committed_message_triggers_improved_workflow_without_periodic_polling(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.agents[0].scheduler.tick_rate_seconds = 5.0
    config.agents[0].generation.tick_rate_seconds = 5.0
    config.chat.max_duration_seconds = 3.0
    engine = ConversationEngine(config)
    await engine.start()
    await engine.submit_message(text="any lunch ideas?")
    workflow_started = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.workflow.started"
        and logged.event.payload["agent_id"] == config.agents[0].id
        and logged.event.payload["trigger_kind"] == "room_message",
        timeout=1.0,
    )
    committed = next(
        logged
        for logged in await engine.export_events()
        if logged.event.subject == "conversation.event.message.committed"
    )
    assert workflow_started.seq > committed.seq
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
    start_debug = next(
        logged
        for logged in await engine.export_events()
        if logged.event.subject == "debug.event.agent.workflow.started"
        and logged.event.payload["trigger_kind"] == "run_start"
    )
    assert start_debug.event.payload["agent_id"] == config.agents[0].id
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_improved_mode_does_not_start_periodic_cognition_tasks(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    engine = ConversationEngine(config)
    await engine.start()
    critical_names = set(engine._critical_tasks.values())  # noqa: SLF001
    assert not any(name.startswith("clock.schedule.") for name in critical_names)
    assert not any(name.startswith("clock.generate.") for name in critical_names)
    assert not any(name.startswith("clock.topic.") for name in critical_names)
    assert not any(name.startswith("clock.evict.") for name in critical_names)
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_improved_mode_does_not_call_scheduler_runtime_with_empty_buffer(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.agents[0].scheduler.tick_rate_seconds = 0.1
    config.agents[0].generation.tick_rate_seconds = 10.0
    runtime = CountingSlowGeneratorRuntime(delay_seconds=0.55)
    engine = ConversationEngine(config, runtime=runtime)
    await engine.start()
    await asyncio.sleep(0.45)
    assert runtime.generator_calls >= 1
    assert runtime.scheduler_calls == 0
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
async def test_buffered_candidate_immediately_triggers_scheduler(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.agents[0].generation.tick_rate_seconds = 10.0
    config.agents[0].scheduler.tick_rate_seconds = 10.0
    config.chat.typing_words_per_second = 100.0
    runtime = CountingSlowGeneratorRuntime(delay_seconds=0.2)
    engine = ConversationEngine(config, runtime=runtime)
    await engine.start()
    opening = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "conversation.event.message.committed"
        and logged.event.payload["sender_kind"] == "agent",
        timeout=2.0,
    )
    assert opening.event.payload["sender_id"] == config.agents[0].id
    assert runtime.scheduler_calls >= 1
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_slow_analyzer_does_not_block_generator_or_scheduler(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.chat.typing_words_per_second = 100.0
    engine = ConversationEngine(config, runtime=SlowAnalyzerRuntime(delay_seconds=0.5))
    await engine.start()
    generator_started = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.call.started"
        and logged.event.payload["worker_kind"] == "generator",
        timeout=0.5,
    )
    scheduler_started = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.call.started"
        and logged.event.payload["worker_kind"] == "scheduler",
        timeout=1.0,
    )
    analyzer_completed = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.call.completed"
        and logged.event.payload["worker_kind"] == "analyzer",
        timeout=2.0,
    )
    assert generator_started.seq < analyzer_completed.seq
    assert scheduler_started.seq < analyzer_completed.seq
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_workflow_completion_is_not_blocked_by_slow_analyzer(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.chat.typing_words_per_second = 100.0
    engine = ConversationEngine(config, runtime=SlowAnalyzerRuntime(delay_seconds=0.5))
    await engine.start()
    workflow_completed = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.workflow.completed"
        and logged.event.payload["agent_id"] == config.agents[0].id,
        timeout=1.5,
    )
    analyzer_completed = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.call.completed"
        and logged.event.payload["worker_kind"] == "analyzer",
        timeout=2.0,
    )
    assert workflow_completed.seq < analyzer_completed.seq
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_workflow_coalesces_quick_messages_to_latest_context(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    engine = ConversationEngine(config, runtime=SlowGeneratorRuntime(delay_seconds=0.2))
    await engine.start()
    await engine.submit_message(text="what should we eat?")
    await engine.submit_message(text="maybe thai or sushi")
    coalesced = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.workflow.coalesced"
        and logged.event.payload["agent_id"] == config.agents[0].id,
        timeout=1.5,
    )
    decided = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject.endswith(".scheduler.decided")
        and logged.event.payload["agent_id"] == config.agents[0].id,
        timeout=2.5,
    )
    committed = [
        logged
        for logged in await engine.export_events()
        if logged.event.subject == "conversation.event.message.committed"
    ]
    assert len(committed) >= 2
    latest_human_committed = next(
        logged
        for logged in reversed(committed)
        if logged.event.payload["sender_kind"] == "human"
    )
    assert coalesced.event.payload["trigger_kind"] == "room_message"
    assert coalesced.event.payload["rerun_pending"] is True
    assert decided.event.payload["source_watermark"] >= latest_human_committed.seq
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_workflow_send_path_survives_slow_generator_projection_lag(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
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
async def test_improved_mode_regenerates_fresh_candidate_if_scheduler_latency_makes_buffer_stale(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.chat.typing_words_per_second = 100.0
    config.agents[0].generation.staleness_window_seconds = 0.05
    engine = ConversationEngine(config, runtime=SlowSchedulerRuntime(delay_seconds=0.12))
    await engine.start()
    reply = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "conversation.event.message.committed"
        and logged.event.payload["sender_kind"] == "agent",
        timeout=3.0,
    )
    discarded = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject.endswith(".candidate.discarded")
        and logged.event.payload["reason"] == "stale_before_send",
        timeout=3.0,
    )
    assert discarded.seq < reply.seq
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_improved_mode_follow_up_reconsideration_can_send_after_initial_wait(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.chat.typing_words_per_second = 100.0
    engine = ConversationEngine(config)
    await engine.start()
    await engine.submit_message(text="thai sounds good to me")
    reply = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "conversation.event.message.committed"
        and logged.event.payload["sender_kind"] == "agent",
        timeout=3.0,
    )
    follow_up = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.workflow.started"
        and logged.event.payload["agent_id"] == config.agents[0].id
        and logged.event.payload["trigger_kind"] == "follow_up",
        timeout=3.0,
    )
    assert follow_up.seq < reply.seq
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_self_authored_committed_message_does_not_trigger_immediate_self_reply(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.chat.typing_words_per_second = 100.0
    engine = ConversationEngine(config)
    await engine.start()
    first_reply = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "conversation.event.message.committed"
        and logged.event.payload["sender_kind"] == "agent",
        timeout=2.0,
    )
    skipped = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.workflow.skipped"
        and logged.event.payload["agent_id"] == config.agents[0].id
        and logged.event.payload["reason"] == "self_message",
        timeout=1.0,
    )
    assert skipped.seq > first_reply.seq
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_analyzer_uses_committed_messages_only_and_topic_ids_persist(improved_config):
    engine = ConversationEngine(improved_config)
    await engine.start()
    await engine.submit_message(text="lunch thai budget")
    committed = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "conversation.event.message.committed"
        and logged.event.payload["text"] == "lunch thai budget",
    )
    first = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "topic.event.alex.snapshot.updated"
        and logged.event.payload["watermark"] >= committed.seq
        and logged.event.payload["dominant_topic_id"] is not None,
    )
    await engine.submit_message(text="thai lunch still sounds budget friendly")
    second = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "topic.event.alex.snapshot.updated"
        and logged.seq > first.seq
        and logged.event.payload["watermark"] > first.event.payload["watermark"]
        and logged.event.payload["dominant_topic_id"] is not None,
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
async def test_immediate_delivery_submits_without_waiting_for_clock(improved_config):
    config = improved_config.model_copy(deep=True)
    config.chat.max_duration_seconds = 6.0
    config.agents = [config.agents[0]]
    engine = ConversationEngine(config)
    await engine.start()
    scheduled = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject.endswith(".delivery.scheduled"),
        timeout=3.0,
    )
    reservation_id = scheduled.event.payload["reservation_id"]
    assert scheduled.event.payload["delay_seconds"] == 0.0
    submitted = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject.endswith(".delivery.submitted")
        and logged.event.payload["reservation_id"] == reservation_id,
        timeout=1.0,
    )
    assert submitted.seq > scheduled.seq
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
    config.agents = [config.agents[0]]
    config.chat.typing_words_per_second = 100.0
    engine = ConversationEngine(config)
    await engine.start()
    requeued = await _wait_for_event(
        engine,
        lambda logged: logged.event.subject.endswith(".candidate.requeued"),
        timeout=3.0,
    )
    assert engine.registry.buffer_for(requeued.event.payload["candidate"]["agent_id"])
    await _shutdown(engine)


@pytest.mark.asyncio
async def test_replay_reconstructs_projection_state_without_rerunning_inference(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = [config.agents[0]]
    config.chat.typing_words_per_second = 100.0
    engine = ConversationEngine(config)
    await engine.start()
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "transport.event.message.acked",
        timeout=3.0,
    )
    original_events = await _shutdown(engine)

    replay_log = EventLog()
    replay_registry = ProjectionRegistry(replay_log, config)
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
async def test_agents_use_distinct_session_keys_and_workspaces(improved_config):
    config = improved_config.model_copy(deep=True)
    config.agents = config.agents[:2]
    runtime = RecordingRuntime()
    engine = ConversationEngine(config, runtime=runtime)
    await engine.start()
    await _wait_for_event(
        engine,
        lambda logged: logged.event.subject == "debug.event.agent.call.started"
        and logged.event.payload["worker_kind"] == "generator"
        and logged.event.payload["agent_id"] == config.agents[1].id,
        timeout=2.0,
    )
    generator_calls = [(agent_id, role_name, session_key, workspace_path) for agent_id, role_name, session_key, workspace_path in runtime.calls if role_name == "generator"]
    assert len(generator_calls) >= 2
    session_keys = {session_key for _, _, session_key, _ in generator_calls}
    workspace_paths = {workspace_path for _, _, _, workspace_path in generator_calls}
    assert len(session_keys) >= 2
    assert len(workspace_paths) >= 2
    assert all("participant:" in session_key for session_key in session_keys if session_key)
    await _shutdown(engine)


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
