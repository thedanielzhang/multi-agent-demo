from __future__ import annotations

import argparse
import asyncio
import contextlib
from collections import deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from uuid import uuid4

import uvicorn
from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

from mafia.config import AppConfig, ModeProfile
from mafia.engine import ConversationEngine, load_config
from mafia.messages import ConversationMessage, SenderKind
from mafia.runtimes import available_runtimes, normalize_agent_runtime, runtime_aliases, validate_runtime_provider
from mafia.transport import available_transports
from mafia.web_pages import chat_page_html, config_page_html


class ChatService:
    def __init__(self, config: AppConfig) -> None:
        self._draft_config = self._canonicalize_config(config)
        self._active_config: AppConfig | None = None
        self._engine: ConversationEngine | None = None
        self._lock = asyncio.Lock()
        self._sockets: set[WebSocket] = set()
        self._participants: dict[WebSocket, dict[str, Any]] = {}
        self._debug_events: deque[dict[str, Any]] = deque(maxlen=200)

    async def start_run(self) -> dict[str, Any]:
        async with self._lock:
            if self._engine and self._engine.registry.run_state() in {"starting", "running", "paused"}:
                return self.status()
            if self._engine is not None:
                await self._engine.close()
            config = self._validated_draft_config()
            self._active_config = config.model_copy(deep=True)
            self._debug_events.clear()
            self._engine = self._new_engine(self._active_config)
            await self._engine.start()
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
        for websocket in websockets:
            with contextlib.suppress(Exception):
                await websocket.close()

    def status(self) -> dict[str, Any]:
        run_state = "idle"
        message_count = 0
        if self._engine is not None:
            run_state = self._engine.registry.run_state()
            message_count = len(self._engine.registry.latest_messages())
        current = self._active_config if run_state in {"starting", "running", "paused"} and self._active_config else self._draft_config
        return {
            "run_state": run_state,
            "mode": current.mode,
            "scenario": current.chat.scenario,
            "message_count": message_count,
            "participant_count": len(self._participants),
            "viewer_count": len(self._sockets),
            "participants": list(self._participants.values()),
            "draft_config": self._draft_config.model_dump(mode="json"),
            "active_config": self._active_config.model_dump(mode="json") if self._active_config else None,
            "runtime_validation": self._runtime_validation(self._draft_config),
            "debug_events": list(self._debug_events),
        }

    def get_draft_config(self) -> dict[str, Any]:
        return self._draft_config.model_dump(mode="json")

    def config_schema(self) -> dict[str, Any]:
        return {
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
        try:
            while True:
                message = await websocket.receive_json()
                message_type = message.get("type")
                if message_type == "join":
                    participant = {
                        "participant_id": message.get("participant_id") or str(uuid4()),
                        "display_name": message.get("display_name") or "Human",
                        "kind": SenderKind.HUMAN,
                    }
                    self._participants[websocket] = participant
                    await websocket.send_json(
                        {
                            "type": "join",
                            "participant": _json_ready(participant),
                            "run_state": self.status()["run_state"],
                        }
                    )
                    continue

                if message_type == "send_message":
                    participant = self._participants.get(websocket)
                    if participant is None:
                        await websocket.send_json({"type": "error", "message": "join required before sending"})
                        continue
                    if self._engine is None or self._engine.registry.run_state() != "running":
                        await websocket.send_json({"type": "error", "message": "room is not running"})
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

                await websocket.send_json({"type": "error", "message": f"unsupported message type: {message_type}"})
        except WebSocketDisconnect:
            self._participants.pop(websocket, None)
            self._sockets.discard(websocket)

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
            "debug.event.agent.call.*",
            self._on_agent_debug_event,
            maxsize=256,
            overflow="drop_oldest",
        )
        return engine

    async def _on_message_committed(self, _subject: str, event) -> None:
        message = ConversationMessage.model_validate(event.payload)
        await self._broadcast(
            {
                "type": "message_committed",
                "message": _message_payload(message),
            }
        )

    async def _on_run_state_changed(self, _subject: str, event) -> None:
        payload = dict(event.payload)
        await self._broadcast(
            {
                "type": "run_state_changed",
                "state": payload["state"],
                "reason": payload.get("reason"),
            }
        )

    async def _on_agent_debug_event(self, _subject: str, event) -> None:
        payload = {
            "type": "debug_event",
            "subject": event.subject,
            "timestamp": event.timestamp.isoformat(),
            "event": _json_ready(dict(event.payload)),
        }
        self._debug_events.append(payload)
        await self._broadcast(payload)

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

    def _canonicalize_config(self, config: AppConfig) -> AppConfig:
        clone = config.model_copy(deep=True)
        clone.runtime.provider = normalize_agent_runtime(clone.runtime.provider)
        return clone

    def _runtime_validation(self, config: AppConfig) -> dict[str, Any]:
        return validate_runtime_provider(config.runtime)

    def _validated_draft_config(self) -> AppConfig:
        validation = self._runtime_validation(self._draft_config)
        if validation["errors"]:
            raise HTTPException(status_code=400, detail="; ".join(validation["errors"]))
        return self._draft_config.model_copy(deep=True)


def create_app(config: AppConfig) -> FastAPI:
    service = ChatService(config)

    @asynccontextmanager
    async def _lifespan(_app: FastAPI):
        _app.state.chat_service = service
        yield
        await service.shutdown()

    app = FastAPI(title="mafia chat service", lifespan=_lifespan)

    @app.get("/", response_class=HTMLResponse)
    async def _chat_page() -> str:
        return chat_page_html()

    @app.get("/config", response_class=HTMLResponse)
    async def _config_page() -> str:
        return config_page_html()

    @app.get("/api/config")
    async def _get_config() -> dict[str, Any]:
        return service.get_draft_config()

    @app.put("/api/config")
    async def _put_config(payload: dict[str, Any]) -> dict[str, Any]:
        return service.update_draft_config(payload)

    @app.get("/api/config/schema")
    async def _get_config_schema() -> dict[str, Any]:
        return service.config_schema()

    @app.get("/api/debug")
    async def _get_debug() -> list[dict[str, Any]]:
        return service.debug_events()

    @app.get("/api/messages")
    async def _get_messages() -> list[dict[str, Any]]:
        return service.committed_messages()

    @app.post("/start")
    async def _start() -> dict[str, Any]:
        return await service.start_run()

    @app.post("/pause")
    async def _pause() -> dict[str, Any]:
        return await service.pause_run()

    @app.post("/resume")
    async def _resume() -> dict[str, Any]:
        return await service.resume_run()

    @app.post("/stop")
    async def _stop() -> dict[str, Any]:
        return await service.stop_run()

    @app.get("/status")
    async def _status() -> dict[str, Any]:
        return service.status()

    @app.websocket("/ws")
    async def _ws(websocket: WebSocket) -> None:
        await service.handle_socket(websocket)

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
    if isinstance(value, SenderKind):
        return value.value
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


__all__ = ["ChatService", "create_app", "main"]


if __name__ == "__main__":
    main()
