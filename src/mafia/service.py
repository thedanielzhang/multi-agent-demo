from __future__ import annotations

import argparse
import asyncio
import contextlib
import re
from collections import deque
from contextlib import asynccontextmanager
from enum import Enum
from pathlib import Path
from typing import Any
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from mafia.config import AppConfig, ModeProfile, RoomMode
from mafia.engine import ConversationEngine, load_config
from mafia.mafia_personas import generate_mafia_personas
from mafia.messages import CommandEnvelope, ConversationMessage, LoggedEvent, MafiaGameStatus, MafiaPhase, MafiaPrivateState, SenderKind
from mafia.runtimes import available_runtimes, normalize_agent_runtime, runtime_aliases, validate_runtime_provider
from mafia.transport import available_transports
from mafia.web_pages import app_shell_html

DEFAULT_ROOM_ID = "main"
_ROOM_ID_RE = re.compile(r"[^a-z0-9]+")


class ChatRoomService:
    def __init__(self, room_id: str, config: AppConfig) -> None:
        self.room_id = room_id
        self.room_title = _room_title(room_id)
        self._draft_config = self._canonicalize_config(config)
        self._active_config: AppConfig | None = None
        self._engine: ConversationEngine | None = None
        self._lock = asyncio.Lock()
        self._sockets: set[WebSocket] = set()
        self._participants: dict[WebSocket, dict[str, Any]] = {}
        self._viewers: dict[WebSocket, dict[str, Any]] = {}
        self._debug_events: deque[dict[str, Any]] = deque(maxlen=300)
        self._viewer_sequence = 0

    async def start_run(self) -> dict[str, Any]:
        async with self._lock:
            if self._engine and self._engine.registry.run_state() in {"starting", "running", "paused"}:
                if self._draft_config.room_mode == RoomMode.MAFIA and self._engine.registry.run_state() == "running":
                    await self._start_mafia_game_from_lobby()
                return self.status()
            if self._engine is not None:
                await self._engine.close()
            config = self._validated_draft_config()
            self._active_config = config.model_copy(deep=True)
            self._debug_events.clear()
            self._engine = self._new_engine(self._active_config)
            await self._engine.start()
            await self._broadcast_presence()
            return self.status()

    async def pause_run(self) -> dict[str, Any]:
        if self._engine and self._engine.registry.run_state() == "running":
            await self._engine.dispatch_command(_run_command("pause"))
        return self.status()

    async def resume_run(self) -> dict[str, Any]:
        if self._engine and self._engine.registry.run_state() == "paused":
            await self._engine.dispatch_command(_run_command("resume"))
        return self.status()

    async def stop_run(self) -> dict[str, Any]:
        if self._engine and self._engine.registry.run_state() not in {"idle", "stopped", "failed"}:
            await self._engine.dispatch_command(_run_command("stop"))
        return self.status()

    async def shutdown(self) -> None:
        if self._engine is not None:
            await self._engine.close()
        websockets = list(self._sockets)
        self._sockets.clear()
        self._participants.clear()
        self._viewers.clear()
        for websocket in websockets:
            with contextlib.suppress(Exception):
                await websocket.close()

    def status(self) -> dict[str, Any]:
        run_state = "idle"
        message_count = 0
        if self._engine is not None:
            run_state = self._engine.registry.run_state()
            message_count = len(self._engine.registry.latest_messages())
        current = (
            self._active_config
            if run_state in {"starting", "running", "paused"} and self._active_config is not None
            else self._draft_config
        )
        return {
            "room_id": self.room_id,
            "room_title": self.room_title,
            "room_path": room_path(self.room_id),
            "room_config_path": room_config_path(self.room_id),
            "share_path": room_path(self.room_id),
            "run_state": run_state,
            "room_mode": current.room_mode,
            "mode": current.mode,
            "scenario": current.chat.scenario,
            "message_count": message_count,
            "participant_count": len(self._participants),
            "viewer_count": len(self._sockets),
            "participants": list(self._participants.values()),
            "viewer_presence": self._viewer_presence(),
            "agents": [
                {
                    "participant_id": agent.id,
                    "display_name": agent.display_name,
                    "kind": "agent",
                }
                for agent in current.agents
            ],
            "draft_config": self._draft_config.model_dump(mode="json"),
            "active_config": self._active_config.model_dump(mode="json") if self._active_config else None,
            "runtime_validation": self._runtime_validation(self._draft_config),
            "mafia_state": _json_ready(self._engine.registry.mafia_public_state().model_dump(mode="json")) if self._engine and self._engine.registry.mafia_public_state() else None,
            "mafia_lobby_spinup": self._mafia_lobby_spinup_status(current, run_state=run_state),
        }

    def summary(self) -> dict[str, Any]:
        status = self.status()
        return {
            "room_id": status["room_id"],
            "room_title": status["room_title"],
            "room_path": status["room_path"],
            "room_mode": status["room_mode"],
            "run_state": status["run_state"],
            "scenario": status["scenario"],
            "message_count": status["message_count"],
            "participant_count": status["participant_count"],
            "viewer_count": status["viewer_count"],
        }

    def get_draft_config(self) -> dict[str, Any]:
        return self._draft_config.model_dump(mode="json")

    def config_schema(self) -> dict[str, Any]:
        return {
            "room_modes": [mode.value for mode in RoomMode],
            "modes": [mode.value for mode in ModeProfile],
            "runtime_providers": list(available_runtimes()),
            "runtime_aliases": runtime_aliases(),
            "transport_providers": list(available_transports()),
            "app_config_schema": AppConfig.model_json_schema(),
        }

    def debug_events(self) -> list[dict[str, Any]]:
        return list(self._debug_events)

    def committed_messages(self) -> list[dict[str, Any]]:
        if self._engine is None:
            return []
        return [_message_payload(message) for message in self._engine.registry.latest_messages()]

    def update_draft_config(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            config = AppConfig.model_validate(payload)
            config = self._canonicalize_config(config)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        validation = self._runtime_validation(config)
        if validation["errors"]:
            raise HTTPException(status_code=400, detail="; ".join(validation["errors"]))
        self._draft_config = config
        return {
            "config": self._draft_config.model_dump(mode="json"),
            "validation": validation,
        }

    async def handle_socket(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self._sockets.add(websocket)
        self._viewer_sequence += 1
        self._viewers[websocket] = {
            "viewer_id": str(uuid4()),
            "connected_order": self._viewer_sequence,
            "ready": False,
            "participant_id": None,
            "display_name": None,
        }
        try:
            await websocket.send_json({"type": "room_snapshot", "status": _json_ready(self.status())})
            await self._broadcast_presence()
            while True:
                message = await websocket.receive_json()
                message_type = message.get("type")
                if message_type == "join":
                    display_name = (message.get("display_name") or "").strip()
                    if self._draft_config.room_mode == RoomMode.MAFIA and not display_name:
                        await websocket.send_json({"type": "error", "message": "name required before joining the lobby"})
                        continue
                    participant_id = message.get("participant_id") or str(uuid4())
                    participant = {
                        "participant_id": participant_id,
                        "display_name": display_name or "Human",
                        "kind": SenderKind.HUMAN,
                    }
                    mafia_public = self._engine.registry.mafia_public_state() if self._engine else None
                    if (
                        self._draft_config.room_mode == RoomMode.MAFIA
                        and mafia_public is not None
                        and mafia_public.game_status != MafiaGameStatus.LOBBY
                    ):
                        existing_state = self._engine.registry.mafia_private_state_for(participant_id)
                        if existing_state is None or existing_state.spectator:
                            participant["kind"] = "spectator"
                    participant = {
                        **participant,
                    }
                    self._participants[websocket] = participant
                    viewer = self._viewers.get(websocket)
                    if viewer is not None:
                        viewer["ready"] = True
                        viewer["participant_id"] = participant["participant_id"]
                        viewer["display_name"] = participant["display_name"]
                    await websocket.send_json(
                        {
                            "type": "join",
                            "participant": _json_ready(participant),
                            "room_id": self.room_id,
                            "run_state": self.status()["run_state"],
                        }
                    )
                    await self._send_player_state(websocket, participant["participant_id"])
                    await self._broadcast_presence()
                    continue

                if message_type == "send_message":
                    participant = self._participants.get(websocket)
                    if participant is None:
                        await websocket.send_json({"type": "error", "message": "join required before sending"})
                        continue
                    if self._engine is None or self._engine.registry.run_state() != "running":
                        await websocket.send_json({"type": "error", "message": "room is not running"})
                        continue
                    if self._draft_config.room_mode == RoomMode.MAFIA:
                        if participant.get("kind") == "spectator":
                            await websocket.send_json({"type": "error", "message": "spectators cannot chat in mafia mode"})
                            continue
                        private_state = self._engine.registry.mafia_private_state_for(participant["participant_id"])
                        if private_state is None or not private_state.can_chat:
                            await websocket.send_json({"type": "error", "message": "chat is disabled for your role in the current phase"})
                            continue
                    await self._engine.submit_message(
                        text=message["text"],
                        sender_id=participant["participant_id"],
                        display_name=participant["display_name"],
                        sender_kind=SenderKind.HUMAN,
                        mentions=message.get("mentions") or [],
                        client_message_id=message.get("client_message_id"),
                        reply_hint=message.get("reply_hint"),
                        metadata=message.get("metadata") or {},
                    )
                    continue

                if message_type == "cast_vote":
                    participant = self._participants.get(websocket)
                    if participant is None:
                        await websocket.send_json({"type": "error", "message": "join required before voting"})
                        continue
                    if self._engine is None or self._engine.registry.run_state() != "running":
                        await websocket.send_json({"type": "error", "message": "room is not running"})
                        continue
                    if self._draft_config.room_mode != RoomMode.MAFIA:
                        await websocket.send_json({"type": "error", "message": "voting is only available in mafia rooms"})
                        continue
                    private_state = self._engine.registry.mafia_private_state_for(participant["participant_id"])
                    if private_state is None or not (private_state.can_vote or private_state.can_act):
                        await websocket.send_json({"type": "error", "message": "you cannot vote right now"})
                        continue
                    target = message.get("target_participant_id")
                    if target is not None and target not in private_state.legal_targets:
                        await websocket.send_json({"type": "error", "message": "invalid vote target"})
                        continue
                    await self._engine.dispatch_command(
                        CommandEnvelope(
                            subject="mafia.command.vote.cast",
                            payload={
                                "participant_id": participant["participant_id"],
                                "target_participant_id": target,
                            },
                        )
                    )
                    continue

                await websocket.send_json({"type": "error", "message": f"unsupported message type: {message_type}"})
        except WebSocketDisconnect:
            pass
        finally:
            self._participants.pop(websocket, None)
            self._sockets.discard(websocket)
            self._viewers.pop(websocket, None)
            await self._broadcast_presence()

    def _new_engine(self, config: AppConfig) -> ConversationEngine:
        engine = ConversationEngine(config.model_copy(deep=True))
        engine.bus.subscribe(
            "conversation.event.message.committed",
            self._on_message_committed,
            maxsize=256,
            overflow="drop_oldest",
        )
        engine.bus.subscribe(
            "run.event.state.changed",
            self._on_run_state_changed,
            maxsize=64,
            overflow="drop_oldest",
        )
        engine.bus.subscribe(
            "debug.event.agent.*",
            self._on_agent_debug_event,
            maxsize=256,
            overflow="drop_oldest",
        )
        engine.bus.subscribe(
            "mafia.event.snapshot.updated",
            self._on_mafia_snapshot_updated,
            maxsize=128,
            overflow="drop_oldest",
        )
        engine.bus.subscribe(
            "mafia.event.vote.revealed",
            self._on_mafia_vote_revealed,
            maxsize=64,
            overflow="drop_oldest",
        )
        engine.bus.subscribe(
            "mafia.event.game.over",
            self._on_mafia_game_over,
            maxsize=64,
            overflow="drop_oldest",
        )
        return engine

    async def _on_message_committed(self, _subject: str, logged_event: LoggedEvent) -> None:
        message = ConversationMessage.model_validate(logged_event.event.payload)
        await self._broadcast(
            {
                "type": "message_committed",
                "message": _message_payload(message),
            }
        )

    async def _on_run_state_changed(self, _subject: str, logged_event: LoggedEvent) -> None:
        payload = dict(logged_event.event.payload)
        await self._broadcast(
            {
                "type": "run_state_changed",
                "state": payload["state"],
                "reason": payload.get("reason"),
            }
        )
        await self._broadcast_presence()

    async def _on_mafia_snapshot_updated(self, _subject: str, logged_event: LoggedEvent) -> None:
        snapshot = self._engine.registry.mafia_public_state() if self._engine else None
        if snapshot is None:
            return
        await self._broadcast(
            {
                "type": "mafia_state_changed",
                "state": _json_ready(snapshot.model_dump(mode="json")),
            }
        )
        for websocket, participant in list(self._participants.items()):
            await self._send_player_state(websocket, participant["participant_id"])
        await self._broadcast_presence()

    async def _on_mafia_vote_revealed(self, _subject: str, logged_event: LoggedEvent) -> None:
        await self._broadcast(
            {
                "type": "mafia_vote_reveal",
                "event": _json_ready(dict(logged_event.event.payload)),
            }
        )

    async def _on_mafia_game_over(self, _subject: str, logged_event: LoggedEvent) -> None:
        await self._broadcast(
            {
                "type": "mafia_game_over",
                "event": _json_ready(dict(logged_event.event.payload)),
            }
        )

    async def _on_agent_debug_event(self, _subject: str, logged_event: LoggedEvent) -> None:
        payload = {
            "type": "debug_event",
            "subject": logged_event.event.subject,
            "timestamp": logged_event.event.timestamp.isoformat(),
            "seq": logged_event.seq,
            "event": _json_ready(dict(logged_event.event.payload)),
        }
        self._debug_events.append(payload)
        await self._broadcast(payload)
        spinup = self._mafia_lobby_spinup_status()
        if spinup and spinup["active"]:
            await self._broadcast_presence()

    async def _broadcast(self, payload: dict[str, Any]) -> None:
        if not self._sockets:
            return
        stale: list[WebSocket] = []
        for websocket in list(self._sockets):
            try:
                await websocket.send_json(_json_ready(payload))
            except Exception:
                stale.append(websocket)
        for websocket in stale:
            self._participants.pop(websocket, None)
            self._sockets.discard(websocket)
            self._viewers.pop(websocket, None)
        if stale:
            await self._broadcast_presence()

    async def _broadcast_presence(self) -> None:
        await self._broadcast(
            {
                "type": "presence_changed",
                "room_id": self.room_id,
                "participant_count": len(self._participants),
                "viewer_count": len(self._sockets),
                "participants": list(self._participants.values()),
                "viewer_presence": self._viewer_presence(),
                "mafia_lobby_spinup": self._mafia_lobby_spinup_status(),
            }
        )

    def _canonicalize_config(self, config: AppConfig) -> AppConfig:
        clone = config.model_copy(deep=True)
        clone.runtime.provider = normalize_agent_runtime(clone.runtime.provider)
        if clone.room_mode == RoomMode.MAFIA:
            clone.mode = ModeProfile.IMPROVED_BUFFERED_ASYNC
            target = clone.mafia.total_players
            if len(clone.agents) != target:
                clone.agents = generate_mafia_personas(self.room_id, target)
        return clone

    def _runtime_validation(self, config: AppConfig) -> dict[str, Any]:
        return validate_runtime_provider(config.runtime)

    def _validated_draft_config(self) -> AppConfig:
        validation = self._runtime_validation(self._draft_config)
        if validation["errors"]:
            raise HTTPException(status_code=400, detail="; ".join(validation["errors"]))
        return self._draft_config.model_copy(deep=True)

    def _viewer_presence(self) -> list[dict[str, Any]]:
        items = sorted(self._viewers.values(), key=lambda item: item["connected_order"])
        public: list[dict[str, Any]] = []
        for viewer in items:
            public.append(
                {
                    "viewer_id": viewer["viewer_id"],
                    "ready": bool(viewer["ready"]),
                    "display_name": viewer["display_name"],
                    "participant_id": viewer["participant_id"],
                }
            )
        return public

    def _mafia_lobby_spinup_status(
        self,
        config: AppConfig | None = None,
        *,
        run_state: str | None = None,
    ) -> dict[str, Any] | None:
        current = config or self._draft_config
        if current.room_mode != RoomMode.MAFIA:
            return None
        effective_run_state = run_state or (self._engine.registry.run_state() if self._engine is not None else "idle")
        public_state = self._engine.registry.mafia_public_state() if self._engine else None
        active = (
            self._engine is not None
            and effective_run_state == "running"
            and public_state is not None
            and public_state.game_status == MafiaGameStatus.LOBBY
            and public_state.phase == MafiaPhase.LOBBY
        )

        ready_count = 0
        failed_count = 0
        agents: list[dict[str, Any]] = []
        for agent in current.agents:
            agent_status = "idle"
            error: str | None = None
            if active and self._engine is not None:
                if self._engine.registry.buffer_for(agent.id):
                    agent_status = "ready"
                    ready_count += 1
                else:
                    latest_debug = self._latest_lobby_spinup_debug(agent.id)
                    if latest_debug and latest_debug.get("subject") == "debug.event.agent.call.failed":
                        agent_status = "failed"
                        failed_count += 1
                        error = str((latest_debug.get("event") or {}).get("error") or "")
                    else:
                        agent_status = "spinning_up"
            payload = {
                "participant_id": agent.id,
                "display_name": agent.display_name,
                "status": agent_status,
            }
            if error:
                payload["error"] = error
            agents.append(payload)

        total_agents = len(current.agents)
        return {
            "active": active,
            "ready": total_agents == 0 or ready_count == total_agents,
            "total_agents": total_agents,
            "ready_count": ready_count,
            "failed_count": failed_count,
            "pending_count": max(0, total_agents - ready_count - failed_count) if active else 0,
            "agents": agents,
        }

    def _latest_lobby_spinup_debug(self, agent_id: str) -> dict[str, Any] | None:
        for entry in reversed(self._debug_events):
            event = entry.get("event") or {}
            invocation = event.get("invocation") or {}
            if event.get("agent_id") != agent_id:
                continue
            if event.get("worker_kind") != "generator":
                continue
            if not invocation.get("mafia_lobby_spinup"):
                continue
            if entry.get("subject") in {
                "debug.event.agent.call.started",
                "debug.event.agent.call.completed",
                "debug.event.agent.call.failed",
            }:
                return entry
        return None

    async def _send_player_state(self, websocket: WebSocket, participant_id: str) -> None:
        if self._engine is None or self._draft_config.room_mode != RoomMode.MAFIA:
            return
        private_state = self._engine.registry.mafia_private_state_for(participant_id)
        if private_state is None:
            private_state = MafiaPrivateState(participant_id=participant_id)
        await websocket.send_json(
            {
                "type": "player_state",
                "state": _json_ready(private_state.model_dump(mode="json")),
            }
        )

    async def _start_mafia_game_from_lobby(self) -> None:
        if self._draft_config.room_mode != RoomMode.MAFIA:
            return
        if self._engine is None or self._engine.registry.run_state() != "running":
            return
        public_state = self._engine.registry.mafia_public_state()
        if public_state is None or public_state.game_status != MafiaGameStatus.LOBBY:
            return
        spinup = self._mafia_lobby_spinup_status(self._active_config or self._draft_config, run_state="running")
        if spinup is not None and not spinup["ready"]:
            blocked = [agent["display_name"] for agent in spinup["agents"] if agent["status"] != "ready"]
            failed = [agent["display_name"] for agent in spinup["agents"] if agent["status"] == "failed"]
            if failed:
                raise HTTPException(
                    status_code=409,
                    detail=f"agents failed to spin up: {', '.join(failed)}",
                )
            raise HTTPException(
                status_code=409,
                detail=f"agents are still spinning up: {', '.join(blocked)}",
            )
        slots = self._eligible_lobby_slots()
        ready = [viewer for viewer in slots if viewer["ready"] and viewer["participant_id"] and viewer["display_name"]]
        humans = [
            {
                "participant_id": viewer["participant_id"],
                "display_name": viewer["display_name"],
            }
            for viewer in ready[: self._draft_config.mafia.total_players]
        ]
        await self._engine.dispatch_command(
            CommandEnvelope(
                subject="mafia.command.game.start",
                payload={"humans": humans},
            )
        )

    def _eligible_lobby_slots(self) -> list[dict[str, Any]]:
        capacity = self._draft_config.mafia.total_players if self._draft_config.room_mode == RoomMode.MAFIA else len(self._viewers)
        viewers = sorted(self._viewers.values(), key=lambda item: item["connected_order"])
        return viewers[:capacity]


class ChatRoomManager:
    def __init__(self, config: AppConfig) -> None:
        self._template_config = config.model_copy(deep=True)
        self._rooms: dict[str, ChatRoomService] = {}

    def template_config(self) -> dict[str, Any]:
        return self._template_config.model_copy(deep=True).model_dump(mode="json")

    def get_room(self, room_id: str | None = None) -> ChatRoomService:
        normalized = normalize_room_id(room_id)
        room = self._rooms.get(normalized)
        if room is None:
            room = ChatRoomService(normalized, self._template_config)
            self._rooms[normalized] = room
        return room

    def has_room(self, room_id: str | None = None) -> bool:
        normalized = normalize_room_id(room_id)
        return normalized in self._rooms

    def require_room(self, room_id: str | None = None) -> ChatRoomService:
        normalized = normalize_room_id(room_id)
        room = self._rooms.get(normalized)
        if room is None:
            raise HTTPException(status_code=404, detail=f"room '{normalized}' not found")
        return room

    def create_room(self, room_id: str | None = None, *, config_payload: dict[str, Any] | None = None) -> ChatRoomService:
        normalized = normalize_room_id(room_id)
        if normalized in self._rooms:
            room = self._rooms[normalized]
        else:
            room = ChatRoomService(normalized, self._template_config)
            self._rooms[normalized] = room
        if config_payload is not None:
            room.update_draft_config(config_payload)
        return room

    def list_rooms(self) -> list[dict[str, Any]]:
        rooms = [room.summary() for room in self._rooms.values()]
        return sorted(
            rooms,
            key=lambda room: (
                room["run_state"] not in {"running", "paused"},
                room["room_id"],
            ),
        )

    async def shutdown(self) -> None:
        await asyncio.gather(*(room.shutdown() for room in self._rooms.values()), return_exceptions=True)


def create_app(config: AppConfig) -> FastAPI:
    manager = ChatRoomManager(config)
    static_dir = Path(__file__).with_name("static")

    @asynccontextmanager
    async def _lifespan(_app: FastAPI):
        _app.state.chat_room_manager = manager
        yield
        await manager.shutdown()

    app = FastAPI(title="mafia chat service", lifespan=_lifespan)
    app.mount("/assets", StaticFiles(directory=static_dir), name="assets")

    @app.get("/", response_class=HTMLResponse)
    async def _lobby_page() -> str:
        return app_shell_html()

    @app.get("/config", response_class=HTMLResponse)
    async def _default_config_page() -> str:
        return app_shell_html()

    @app.get("/rooms/{room_id}/config", response_class=HTMLResponse)
    async def _room_config_page(room_id: str) -> str:
        manager.require_room(room_id)
        return app_shell_html()

    @app.get("/rooms/{room_id}", response_class=HTMLResponse)
    async def _room_chat_page(room_id: str) -> str:
        manager.require_room(room_id)
        return app_shell_html()

    @app.get("/api/rooms")
    async def _list_rooms() -> dict[str, Any]:
        return {"rooms": manager.list_rooms()}

    @app.get("/api/room-template")
    async def _room_template() -> dict[str, Any]:
        return manager.template_config()

    @app.post("/api/rooms")
    async def _create_room(payload: dict[str, Any] | None = None) -> dict[str, Any]:
        requested = None if payload is None else payload.get("room_id")
        config_payload = None if payload is None else payload.get("config")
        room = manager.create_room(requested or _random_room_id(), config_payload=config_payload)
        if room.status()["room_mode"] == RoomMode.MAFIA:
            await room.start_run()
        return room.summary()

    @app.get("/api/rooms/{room_id}/config")
    async def _get_room_config(room_id: str) -> dict[str, Any]:
        return manager.require_room(room_id).get_draft_config()

    @app.put("/api/rooms/{room_id}/config")
    async def _put_room_config(room_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        return manager.require_room(room_id).update_draft_config(payload)

    @app.get("/api/rooms/{room_id}/config/schema")
    async def _get_room_config_schema(room_id: str) -> dict[str, Any]:
        return manager.require_room(room_id).config_schema()

    @app.get("/api/rooms/{room_id}/debug")
    async def _get_room_debug(room_id: str) -> list[dict[str, Any]]:
        return manager.require_room(room_id).debug_events()

    @app.get("/api/rooms/{room_id}/messages")
    async def _get_room_messages(room_id: str) -> list[dict[str, Any]]:
        return manager.require_room(room_id).committed_messages()

    @app.get("/api/rooms/{room_id}/status")
    async def _get_room_status(room_id: str) -> dict[str, Any]:
        return manager.require_room(room_id).status()

    @app.post("/api/rooms/{room_id}/start")
    async def _start_room(room_id: str) -> dict[str, Any]:
        return await manager.require_room(room_id).start_run()

    @app.post("/api/rooms/{room_id}/pause")
    async def _pause_room(room_id: str) -> dict[str, Any]:
        return await manager.require_room(room_id).pause_run()

    @app.post("/api/rooms/{room_id}/resume")
    async def _resume_room(room_id: str) -> dict[str, Any]:
        return await manager.require_room(room_id).resume_run()

    @app.post("/api/rooms/{room_id}/stop")
    async def _stop_room(room_id: str) -> dict[str, Any]:
        return await manager.require_room(room_id).stop_run()

    @app.get("/api/config")
    async def _get_config() -> dict[str, Any]:
        return manager.get_room(DEFAULT_ROOM_ID).get_draft_config()

    @app.put("/api/config")
    async def _put_config(payload: dict[str, Any]) -> dict[str, Any]:
        return manager.get_room(DEFAULT_ROOM_ID).update_draft_config(payload)

    @app.get("/api/config/schema")
    async def _get_config_schema() -> dict[str, Any]:
        return manager.get_room(DEFAULT_ROOM_ID).config_schema()

    @app.get("/api/debug")
    async def _get_debug() -> list[dict[str, Any]]:
        return manager.get_room(DEFAULT_ROOM_ID).debug_events()

    @app.get("/api/messages")
    async def _get_messages() -> list[dict[str, Any]]:
        return manager.get_room(DEFAULT_ROOM_ID).committed_messages()

    @app.get("/status")
    async def _status() -> dict[str, Any]:
        return manager.get_room(DEFAULT_ROOM_ID).status()

    @app.post("/start")
    async def _start() -> dict[str, Any]:
        return await manager.get_room(DEFAULT_ROOM_ID).start_run()

    @app.post("/pause")
    async def _pause() -> dict[str, Any]:
        return await manager.get_room(DEFAULT_ROOM_ID).pause_run()

    @app.post("/resume")
    async def _resume() -> dict[str, Any]:
        return await manager.get_room(DEFAULT_ROOM_ID).resume_run()

    @app.post("/stop")
    async def _stop() -> dict[str, Any]:
        return await manager.get_room(DEFAULT_ROOM_ID).stop_run()

    @app.websocket("/ws")
    async def _ws_default(websocket: WebSocket) -> None:
        await manager.get_room(DEFAULT_ROOM_ID).handle_socket(websocket)

    @app.websocket("/ws/{room_id}")
    async def _ws_room(websocket: WebSocket, room_id: str) -> None:
        if not manager.has_room(room_id):
            await websocket.accept()
            await websocket.send_json({"type": "error", "message": "room not found"})
            await websocket.close(code=4404)
            return
        await manager.require_room(room_id).handle_socket(websocket)

    return app


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mafia-service")
    parser.add_argument("config", help="Path to a JSON or YAML config file")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    config = load_config(Path(args.config))
    uvicorn.run(create_app(config), host=args.host, port=args.port)


def _run_command(name: str):
    from mafia.messages import CommandEnvelope

    return CommandEnvelope(subject=f"run.command.{name}")


def normalize_room_id(room_id: str | None) -> str:
    raw = (room_id or DEFAULT_ROOM_ID).strip().lower()
    slug = _ROOM_ID_RE.sub("-", raw).strip("-")
    return slug or DEFAULT_ROOM_ID


def room_path(room_id: str) -> str:
    return f"/rooms/{normalize_room_id(room_id)}"


def room_config_path(room_id: str) -> str:
    return f"{room_path(room_id)}/config"


def _room_title(room_id: str) -> str:
    words = normalize_room_id(room_id).replace("-", " ").split()
    if not words:
        return "Main Room"
    return " ".join(word.capitalize() for word in words)


def _random_room_id() -> str:
    return f"room-{uuid4().hex[:8]}"


def _message_payload(message: ConversationMessage) -> dict[str, Any]:
    return {
        "message_id": message.message_id,
        "client_message_id": message.client_message_id,
        "participant_id": message.sender_id,
        "display_name": message.display_name,
        "kind": message.sender_kind,
        "text": message.text,
        "created_at": message.created_at.isoformat(),
        "sequence_no": message.sequence_no,
        "mentions": list(message.mentions),
        "reply_hint": message.reply_hint,
        "metadata": dict(message.metadata),
    }


def _json_ready(value: Any) -> Any:
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


ChatService = ChatRoomService

__all__ = [
    "ChatRoomManager",
    "ChatRoomService",
    "ChatService",
    "create_app",
    "main",
    "normalize_room_id",
    "room_config_path",
    "room_path",
]


if __name__ == "__main__":
    main()
