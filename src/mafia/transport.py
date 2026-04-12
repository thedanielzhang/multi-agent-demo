from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from mafia.config import TransportConfig
from mafia.messages import CommandEnvelope, MessagePayload, make_event


class TransportEngineProtocol(Protocol):
    async def append_event(self, event): ...
    async def dispatch_command(self, command: CommandEnvelope) -> None: ...

    @property
    def registry(self): ...


class MessageTransport(Protocol):
    name: str

    async def handle_send(self, command: CommandEnvelope) -> None: ...


TransportFactory = Callable[[TransportEngineProtocol, TransportConfig], MessageTransport]
_TRANSPORT_FACTORIES: dict[str, TransportFactory] = {}


def register_transport(name: str, factory: TransportFactory) -> None:
    _TRANSPORT_FACTORIES[name] = factory


def available_transports() -> tuple[str, ...]:
    return tuple(sorted(_TRANSPORT_FACTORIES))


def build_transport(engine: TransportEngineProtocol, config: TransportConfig) -> MessageTransport:
    factory = _TRANSPORT_FACTORIES.get(config.provider)
    if factory is None:
        supported = ", ".join(available_transports()) or "none"
        raise ValueError(
            f"Unsupported transport '{config.provider}'. Supported values: {supported}"
        )
    return factory(engine, config)


class LoopbackTransport:
    name = "loopback"

    def __init__(self, engine: TransportEngineProtocol, config: TransportConfig) -> None:
        self._engine = engine
        self._config = config

    async def handle_send(self, command: CommandEnvelope) -> None:
        payload = dict(command.payload)
        metadata = dict(payload.get("metadata", {}))
        metadata.setdefault("transport", self.name)
        message_payload = MessagePayload(
            client_message_id=payload["client_message_id"],
            sender_id=payload["sender_id"],
            sender_kind=payload["sender_kind"],
            display_name=payload["display_name"],
            text=payload["text"],
            mentions=payload.get("mentions", []),
            reply_hint=payload.get("reply_hint"),
            metadata=metadata,
        )
        await self._engine.dispatch_command(
            CommandEnvelope(
                subject="conversation.command.message.submit",
                correlation_id=command.correlation_id,
                causation_id=command.command_id,
                payload=message_payload.model_dump(mode="json"),
            )
        )
        committed = self._engine.registry.message_by_client_message_id(payload["client_message_id"])
        if committed is None:
            raise RuntimeError(
                f"loopback transport could not find committed message for {payload['client_message_id']}"
            )
        await self._engine.append_event(
            make_event(
                "transport.event.message.acked",
                command=command,
                payload={
                    "transport": self.name,
                    "reservation_id": payload["reservation_id"],
                    "candidate_id": payload["candidate_id"],
                    "client_message_id": payload["client_message_id"],
                    "message_id": committed.message_id,
                },
            )
        )
        await self._engine.dispatch_command(
            CommandEnvelope(
                subject=f"agent.command.{payload['agent_id']}.delivery.transport_acked",
                correlation_id=command.correlation_id,
                causation_id=command.command_id,
                payload={
                    "agent_id": payload["agent_id"],
                    "reservation_id": payload["reservation_id"],
                    "candidate_id": payload["candidate_id"],
                    "message_id": committed.message_id,
                },
            )
        )


register_transport("loopback", lambda engine, config: LoopbackTransport(engine, config))


__all__ = [
    "LoopbackTransport",
    "MessageTransport",
    "available_transports",
    "build_transport",
    "register_transport",
]
