from __future__ import annotations

import random
from datetime import timedelta
from typing import Any, Protocol
from uuid import uuid4

from mafia.messages import (
    CommandEnvelope,
    ConversationMessage,
    EventEnvelope,
    LoggedEvent,
    MafiaFaction,
    MafiaGameSnapshot,
    MafiaGameStatus,
    MafiaPhase,
    MafiaPlayerRecord,
    MafiaRevealRecord,
    MafiaRole,
    SenderKind,
    make_event,
    utc_now,
)


class MafiaControllerEngineProtocol(Protocol):
    run_id: str
    config: Any
    registry: Any
    clock: Any
    _agent_workers: dict[str, Any]

    async def append_event(self, event: EventEnvelope) -> LoggedEvent: ...

    async def append_event_and_wait(self, event: EventEnvelope) -> LoggedEvent: ...

    async def dispatch_command(self, command: CommandEnvelope) -> None: ...

    async def enqueue_command(self, command: CommandEnvelope) -> None: ...


def mafia_count_for_players(player_count: int) -> int:
    if player_count <= 0:
        return 0
    # Original Mafia rules use roughly one-third of the table as mafia,
    # rounded to the nearest whole player.
    rounded_third = int((player_count / 3) + 0.5)
    return max(1, min(player_count - 1, rounded_third))


def _normalized_display_name(name: str) -> str:
    return " ".join(name.split()).strip().casefold()


def _dedupe_display_name(name: str, used_names: set[str]) -> str:
    base = " ".join(name.split()).strip() or "Player"
    candidate = base
    suffix = 2
    while _normalized_display_name(candidate) in used_names:
        candidate = f"{base} {suffix}"
        suffix += 1
    used_names.add(_normalized_display_name(candidate))
    return candidate


class MafiaGameController:
    def __init__(self, engine: MafiaControllerEngineProtocol) -> None:
        self.engine = engine

    async def initialize_lobby(self, command: CommandEnvelope | None = None) -> None:
        snapshot = MafiaGameSnapshot(
            game_status=MafiaGameStatus.LOBBY,
            phase=MafiaPhase.LOBBY,
            phase_started_at=utc_now(),
            phase_ends_at=None,
            total_players=self.engine.config.mafia.total_players,
        )
        await self._append_snapshot(snapshot, command=command)
        await self._emit_system_message(
            "The room is open. Join with a name to claim a seat before the game begins.",
            command=command,
            metadata={"mafia_system": "lobby_opened"},
        )

    async def start_game(self, humans: list[dict[str, Any]], command: CommandEnvelope | None = None) -> None:
        current = self.engine.registry.mafia_snapshot()
        if current is not None and current.game_status == MafiaGameStatus.ACTIVE:
            return
        total_players = self.engine.config.mafia.total_players
        selected_humans = humans[:total_players]
        selected_ids = {human["participant_id"] for human in selected_humans}
        filler_agents = [agent for agent in self.engine.config.agents if agent.id not in selected_ids][: max(0, total_players - len(selected_humans))]
        players: list[MafiaPlayerRecord] = []
        used_names: set[str] = set()
        seat_index = 0
        for human in selected_humans:
            display_name = " ".join(str(human["display_name"]).split()).strip() or "Human"
            used_names.add(_normalized_display_name(display_name))
            players.append(
                MafiaPlayerRecord(
                    participant_id=human["participant_id"],
                    display_name=display_name,
                    is_human=True,
                    seat_index=seat_index,
                    connected=True,
                )
            )
            seat_index += 1
        for agent in filler_agents:
            display_name = _dedupe_display_name(agent.display_name, used_names)
            if display_name != agent.display_name:
                agent.display_name = display_name
            players.append(
                MafiaPlayerRecord(
                    participant_id=agent.id,
                    display_name=display_name,
                    is_human=False,
                    seat_index=seat_index,
                    connected=True,
                )
            )
            seat_index += 1
        if not players:
            return
        mafia_count = mafia_count_for_players(len(players))
        roles = ([MafiaRole.MAFIA] * mafia_count) + ([MafiaRole.TOWN] * max(0, len(players) - mafia_count))
        rng = random.Random(f"{self.engine.run_id}:{','.join(player.participant_id for player in players)}")
        rng.shuffle(roles)
        assigned_players: list[MafiaPlayerRecord] = []
        for player, role in zip(players, roles, strict=False):
            faction = MafiaFaction.MAFIA if role == MafiaRole.MAFIA else MafiaFaction.TOWN
            assigned_players.append(player.model_copy(update={"role": role, "faction": faction}))
        snapshot = MafiaGameSnapshot(
            game_status=MafiaGameStatus.ACTIVE,
            phase=MafiaPhase.DAY_DISCUSSION,
            phase_started_at=utc_now(),
            phase_ends_at=utc_now() + timedelta(seconds=self.engine.config.mafia.day_discussion_seconds),
            total_players=total_players,
            round_no=1,
            players=assigned_players,
            ready_humans=[human["participant_id"] for human in selected_humans],
        )
        await self._append_snapshot(snapshot, command=command)
        await self._emit_system_message(
            f"Day 1 begins. There are {len(assigned_players)} players at the table.",
            command=command,
            metadata={"mafia_system": "phase_start", "phase": MafiaPhase.DAY_DISCUSSION.value},
        )
        await self._schedule_phase_end(snapshot, command=command)

    async def advance_phase(self, command: CommandEnvelope | None = None) -> None:
        snapshot = self.engine.registry.mafia_snapshot()
        if snapshot is None or snapshot.game_status != MafiaGameStatus.ACTIVE:
            return
        now = utc_now()
        if snapshot.phase == MafiaPhase.DAY_DISCUSSION:
            next_snapshot = snapshot.model_copy(
                update={
                    "phase": MafiaPhase.DAY_VOTE,
                    "phase_started_at": now,
                    "phase_ends_at": now + timedelta(seconds=self.engine.config.mafia.day_vote_seconds),
                    "day_votes": {},
                }
            )
            await self._append_snapshot(next_snapshot, command=command)
            await self._emit_system_message(
                "Day voting is open. Cast one secret vote before the timer ends.",
                command=command,
                metadata={"mafia_system": "phase_start", "phase": MafiaPhase.DAY_VOTE.value},
            )
            await self._schedule_phase_end(next_snapshot, command=command)
            await self._trigger_agent_votes(next_snapshot)
            return
        if snapshot.phase == MafiaPhase.DAY_VOTE:
            next_snapshot, revealed = self._resolve_day_vote(snapshot)
            await self._append_snapshot(next_snapshot, command=command)
            await self.engine.append_event(
                make_event("mafia.event.vote.revealed", command=command, payload=revealed)
            )
            await self._emit_system_message(
                revealed["summary"],
                command=command,
                metadata={"mafia_system": "day_reveal"},
            )
            winner = self._winner(next_snapshot)
            if winner is not None:
                await self._finish_game(next_snapshot, winner, command=command)
                return
            reveal_snapshot = next_snapshot.model_copy(
                update={
                    "phase": MafiaPhase.DAY_REVEAL,
                    "phase_started_at": now,
                    "phase_ends_at": now + timedelta(seconds=self.engine.config.mafia.day_reveal_seconds),
                    "day_votes": {},
                }
            )
            await self._append_snapshot(reveal_snapshot, command=command)
            await self._schedule_phase_end(reveal_snapshot, command=command)
            return
        if snapshot.phase == MafiaPhase.DAY_REVEAL:
            next_snapshot = snapshot.model_copy(
                update={
                    "phase": MafiaPhase.NIGHT_ACTION,
                    "phase_started_at": now,
                    "phase_ends_at": now + timedelta(seconds=self.engine.config.mafia.night_action_seconds),
                    "night_votes": {},
                }
            )
            await self._append_snapshot(next_snapshot, command=command)
            await self._emit_system_message(
                "Night falls. The town sleeps while the mafia choose a target.",
                command=command,
                metadata={"mafia_system": "phase_start", "phase": MafiaPhase.NIGHT_ACTION.value},
            )
            await self._schedule_phase_end(next_snapshot, command=command)
            await self._trigger_agent_votes(next_snapshot)
            return
        if snapshot.phase == MafiaPhase.NIGHT_ACTION:
            next_snapshot, revealed = self._resolve_night_vote(snapshot)
            await self._append_snapshot(next_snapshot, command=command)
            await self._emit_system_message(
                revealed["summary"],
                command=command,
                metadata={"mafia_system": "night_reveal"},
            )
            winner = self._winner(next_snapshot)
            if winner is not None:
                await self._finish_game(next_snapshot, winner, command=command)
                return
            reveal_snapshot = next_snapshot.model_copy(
                update={
                    "phase": MafiaPhase.NIGHT_REVEAL,
                    "phase_started_at": now,
                    "phase_ends_at": now + timedelta(seconds=self.engine.config.mafia.night_reveal_seconds),
                    "night_votes": {},
                }
            )
            await self._append_snapshot(reveal_snapshot, command=command)
            await self._schedule_phase_end(reveal_snapshot, command=command)
            return
        if snapshot.phase == MafiaPhase.NIGHT_REVEAL:
            next_snapshot = snapshot.model_copy(
                update={
                    "phase": MafiaPhase.DAY_DISCUSSION,
                    "phase_started_at": now,
                    "phase_ends_at": now + timedelta(seconds=self.engine.config.mafia.day_discussion_seconds),
                    "round_no": snapshot.round_no + 1,
                    "day_votes": {},
                    "night_votes": {},
                }
            )
            await self._append_snapshot(next_snapshot, command=command)
            await self._emit_system_message(
                f"Day {next_snapshot.round_no} begins. Discuss before the next vote.",
                command=command,
                metadata={"mafia_system": "phase_start", "phase": MafiaPhase.DAY_DISCUSSION.value},
            )
            await self._schedule_phase_end(next_snapshot, command=command)

    async def cast_vote(self, participant_id: str, target_participant_id: str | None, command: CommandEnvelope | None = None) -> bool:
        snapshot = self.engine.registry.mafia_snapshot()
        if snapshot is None or snapshot.game_status != MafiaGameStatus.ACTIVE:
            return False
        private_state = snapshot.private_state_for(participant_id)
        if target_participant_id is not None and target_participant_id not in private_state.legal_targets:
            return False
        if snapshot.phase == MafiaPhase.DAY_VOTE and private_state.can_vote:
            updated = dict(snapshot.day_votes)
            if target_participant_id is None:
                updated.pop(participant_id, None)
            else:
                updated[participant_id] = target_participant_id
            next_snapshot = snapshot.model_copy(update={"day_votes": updated})
            await self._append_snapshot(next_snapshot, command=command)
            return True
        if snapshot.phase == MafiaPhase.NIGHT_ACTION and private_state.can_act:
            updated = dict(snapshot.night_votes)
            if target_participant_id is None:
                updated.pop(participant_id, None)
            else:
                updated[participant_id] = target_participant_id
            next_snapshot = snapshot.model_copy(update={"night_votes": updated})
            await self._append_snapshot(next_snapshot, command=command)
            return True
        return False

    async def _append_snapshot(self, snapshot: MafiaGameSnapshot, *, command: CommandEnvelope | None = None) -> None:
        await self.engine.append_event_and_wait(
            make_event(
                "mafia.event.snapshot.updated",
                command=command,
                payload=snapshot,
            )
        )

    async def _emit_system_message(self, text: str, *, command: CommandEnvelope | None = None, metadata: dict[str, Any] | None = None) -> None:
        message = ConversationMessage(
            message_id=str(uuid4()),
            client_message_id=str(uuid4()),
            sender_id="system",
            sender_kind=SenderKind.SYSTEM,
            display_name="System",
            text=text,
            created_at=utc_now(),
            sequence_no=len(self.engine.registry.latest_messages()) + 1,
            metadata=metadata or {},
        )
        await self.engine.append_event_and_wait(
            make_event("conversation.event.message.committed", command=command, payload=message)
        )

    async def _schedule_phase_end(self, snapshot: MafiaGameSnapshot, *, command: CommandEnvelope | None = None) -> None:
        if snapshot.phase_ends_at is None:
            return
        delay_seconds = max(0.0, (snapshot.phase_ends_at - utc_now()).total_seconds())
        await self.engine.clock.schedule_command(
            delay_seconds,
            CommandEnvelope(
                subject="mafia.command.phase.advance",
                correlation_id=command.correlation_id if command else self.engine.run_id,
                causation_id=command.command_id if command else None,
                payload={
                    "expected_phase": snapshot.phase.value,
                    "round_no": snapshot.round_no,
                },
            ),
        )

    async def _trigger_agent_votes(self, snapshot: MafiaGameSnapshot) -> None:
        for player in snapshot.players:
            if player.is_human or not player.alive:
                continue
            private_state = snapshot.private_state_for(player.participant_id)
            if not (private_state.can_vote or private_state.can_act):
                continue
            # Queue vote workers asynchronously so a slow model call cannot keep
            # the phase-advance command occupied past the visible phase timer.
            await self.engine.enqueue_command(
                CommandEnvelope(
                    subject=f"agent.command.{player.participant_id}.mafia.vote",
                    payload={"phase": snapshot.phase.value, "round_no": snapshot.round_no},
                )
            )

    def _resolve_day_vote(self, snapshot: MafiaGameSnapshot) -> tuple[MafiaGameSnapshot, dict[str, Any]]:
        counts = self._counts(snapshot.day_votes)
        if not counts:
            summary = "The town could not agree on a target. No one is eliminated."
            revealed = dict(
                phase=MafiaPhase.DAY_VOTE.value,
                votes=dict(snapshot.day_votes),
                eliminated_participant_id=None,
                eliminated_faction=None,
                summary=summary,
            )
            return snapshot, revealed
        best_target, best_votes = max(counts.items(), key=lambda item: item[1])
        tied = [target for target, count in counts.items() if count == best_votes]
        if len(tied) > 1:
            summary = "The day vote ends in a tie. No one is eliminated."
            revealed = dict(
                phase=MafiaPhase.DAY_VOTE.value,
                votes=dict(snapshot.day_votes),
                eliminated_participant_id=None,
                eliminated_faction=None,
                summary=summary,
            )
            return snapshot, revealed
        next_players: list[MafiaPlayerRecord] = []
        eliminated_name: str | None = None
        eliminated_faction: MafiaFaction | None = None
        for player in snapshot.players:
            if player.participant_id == best_target:
                next_players.append(player.model_copy(update={"alive": False}))
                eliminated_name = player.display_name
                eliminated_faction = player.faction
            else:
                next_players.append(player)
        reveal = MafiaRevealRecord(
            phase=MafiaPhase.DAY_REVEAL,
            participant_id=best_target,
            display_name=eliminated_name,
            faction=eliminated_faction,
            eliminated=True,
            reason="day_vote",
        )
        next_snapshot = snapshot.model_copy(update={"players": next_players, "revealed_eliminations": [*snapshot.revealed_eliminations, reveal]})
        faction_label = eliminated_faction.value if eliminated_faction is not None else "unknown"
        summary = f"The town votes out {eliminated_name or best_target}. They were {faction_label}."
        revealed = dict(
            phase=MafiaPhase.DAY_VOTE.value,
            votes=dict(snapshot.day_votes),
            eliminated_participant_id=best_target,
            eliminated_faction=faction_label,
            summary=summary,
        )
        return next_snapshot, revealed

    def _resolve_night_vote(self, snapshot: MafiaGameSnapshot) -> tuple[MafiaGameSnapshot, dict[str, Any]]:
        counts = self._counts(snapshot.night_votes)
        if not counts:
            summary = "Night passes without a kill."
            return snapshot, dict(
                phase=MafiaPhase.NIGHT_ACTION.value,
                votes={},
                eliminated_participant_id=None,
                eliminated_faction=None,
                summary=summary,
            )
        best_target, best_votes = max(counts.items(), key=lambda item: item[1])
        tied = [target for target, count in counts.items() if count == best_votes]
        if len(tied) > 1:
            summary = "The mafia split their choice. No one dies overnight."
            return snapshot, dict(
                phase=MafiaPhase.NIGHT_ACTION.value,
                votes=dict(snapshot.night_votes),
                eliminated_participant_id=None,
                eliminated_faction=None,
                summary=summary,
            )
        next_players: list[MafiaPlayerRecord] = []
        eliminated_name: str | None = None
        eliminated_faction: MafiaFaction | None = None
        for player in snapshot.players:
            if player.participant_id == best_target:
                next_players.append(player.model_copy(update={"alive": False}))
                eliminated_name = player.display_name
                eliminated_faction = player.faction
            else:
                next_players.append(player)
        reveal = MafiaRevealRecord(
            phase=MafiaPhase.NIGHT_REVEAL,
            participant_id=best_target,
            display_name=eliminated_name,
            faction=eliminated_faction,
            eliminated=True,
            reason="night_kill",
        )
        next_snapshot = snapshot.model_copy(update={"players": next_players, "revealed_eliminations": [*snapshot.revealed_eliminations, reveal]})
        faction_label = eliminated_faction.value if eliminated_faction is not None else "unknown"
        summary = f"At dawn, the group finds that {eliminated_name or best_target} was eliminated overnight. They were {faction_label}."
        return next_snapshot, dict(
            phase=MafiaPhase.NIGHT_ACTION.value,
            votes=dict(snapshot.night_votes),
            eliminated_participant_id=best_target,
            eliminated_faction=faction_label,
            summary=summary,
        )

    def _winner(self, snapshot: MafiaGameSnapshot) -> MafiaFaction | None:
        living_mafia = sum(1 for player in snapshot.players if player.alive and player.faction == MafiaFaction.MAFIA)
        living_town = sum(1 for player in snapshot.players if player.alive and player.faction == MafiaFaction.TOWN)
        if living_mafia <= 0:
            return MafiaFaction.TOWN
        if living_mafia >= living_town:
            return MafiaFaction.MAFIA
        return None

    async def _finish_game(self, snapshot: MafiaGameSnapshot, winner: MafiaFaction, *, command: CommandEnvelope | None = None) -> None:
        winning_ids = [player.participant_id for player in snapshot.players if player.faction == winner]
        game_over = snapshot.model_copy(
            update={
                "game_status": MafiaGameStatus.GAME_OVER,
                "winner": winner,
                "winning_participant_ids": winning_ids,
                "phase_started_at": utc_now(),
                "phase_ends_at": None,
            }
        )
        await self._append_snapshot(game_over, command=command)
        payload = {
            "winner": winner.value,
            "winning_participant_ids": winning_ids,
        }
        await self.engine.append_event(make_event("mafia.event.game.over", command=command, payload=payload))
        await self._emit_system_message(
            f"Game over. {winner.value.capitalize()} wins.",
            command=command,
            metadata={"mafia_system": "game_over", "winner": winner.value},
        )

    def _counts(self, votes: dict[str, str]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for target in votes.values():
            counts[target] = counts.get(target, 0) + 1
        return counts
