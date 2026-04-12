from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from mafia.compose_compat import AgentSession, SessionStore, Workspace


class InMemorySessionStore(SessionStore):
    """Small session store used by the local chat service runtimes."""

    def __init__(self) -> None:
        self._sessions: dict[str, AgentSession] = {}

    async def load(self, session_key: str) -> AgentSession | None:
        session = self._sessions.get(session_key)
        if session is None:
            return None
        return session.model_copy(deep=True)

    async def save(self, session: AgentSession) -> None:
        self._sessions[session.session_key] = session.model_copy(deep=True)


@dataclass(slots=True)
class RuntimeContext:
    session_store: SessionStore | None = None
    on_message: Callable[[Any], None] | None = None
    interactive_roles: set[str] = field(default_factory=set)


def build_workspace(run_id: str, root: Path | None = None) -> Workspace:
    base = (root or Path.cwd()) / ".mafia-workspaces" / run_id
    base.mkdir(parents=True, exist_ok=True)
    return Workspace(
        id=f"mafia-run-{run_id}",
        path=base,
        metadata={"service": "mafia", "run_id": run_id},
    )


__all__ = ["InMemorySessionStore", "RuntimeContext", "build_workspace"]
