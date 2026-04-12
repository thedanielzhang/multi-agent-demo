from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

try:  # pragma: no cover - exercised when iriai-compose is installed
    from iriai_compose import AgentActor, Role
    from iriai_compose.runner import AgentRuntime
    from iriai_compose.storage import AgentSession, SessionStore
    from iriai_compose.workflow import Workspace
except ImportError:  # pragma: no cover - local fallback for hackathon development
    @dataclass
    class Role:
        name: str
        prompt: str
        tools: list[str] = field(default_factory=list)
        model: str | None = None
        effort: str | None = None
        metadata: dict[str, Any] = field(default_factory=dict)

    @dataclass
    class AgentActor:
        name: str
        role: Role
        context_keys: list[str] = field(default_factory=list)

    class AgentRuntime(ABC):
        """Local compatibility shim for the upstream iriai-compose runtime contract."""

        name: str

        @abstractmethod
        async def invoke(
            self,
            role: Role,
            prompt: str,
            *,
            output_type: type[BaseModel] | None = None,
            workspace: Any | None = None,
            session_key: str | None = None,
        ) -> str | BaseModel: ...

    class AgentSession(BaseModel):
        session_key: str
        session_id: str | None = None
        metadata: dict[str, Any] = Field(default_factory=dict)

    class SessionStore(ABC):
        @abstractmethod
        async def load(self, session_key: str) -> AgentSession | None: ...

        @abstractmethod
        async def save(self, session: AgentSession) -> None: ...

    class Workspace(BaseModel):
        id: str
        path: Path
        branch: str | None = None
        metadata: dict[str, Any] = Field(default_factory=dict)


__all__ = [
    "AgentActor",
    "AgentRuntime",
    "AgentSession",
    "Role",
    "SessionStore",
    "Workspace",
]
