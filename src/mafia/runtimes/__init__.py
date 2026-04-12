from __future__ import annotations

import importlib.util
import shutil
from collections.abc import Callable
from typing import Any

from mafia.compose_compat import AgentRuntime
from mafia.config import RuntimeConfig
from mafia.runtime_support import RuntimeContext
from mafia.runtimes.claude import ClaudeAgentRuntime
from mafia.runtimes.codex import CodexAgentRuntime
from mafia.runtimes.scripted import ScriptedAgentRuntime

RuntimeFactory = Callable[[RuntimeConfig, RuntimeContext], AgentRuntime]

_RUNTIME_ALIASES = {
    "anthropic": "claude",
    "claude": "claude",
    "openai": "codex",
    "codex": "codex",
    "scripted": "scripted",
}
_RUNTIME_FACTORIES: dict[str, RuntimeFactory] = {}


def _default_model_for(runtime_name: str) -> str:
    if runtime_name == "claude":
        return "claude-haiku-4-5-20251001"
    if runtime_name == "codex":
        return "gpt-5"
    return "mock-model"


def register_runtime(name: str, factory: RuntimeFactory) -> None:
    _RUNTIME_FACTORIES[name] = factory


def normalize_agent_runtime(name: str | None) -> str:
    raw = (name or "scripted").strip().lower()
    resolved = _RUNTIME_ALIASES.get(raw)
    if resolved is None:
        supported = ", ".join(available_runtimes()) or "none"
        raise ValueError(
            f"Unsupported runtime '{raw}'. Supported values: {supported}"
        )
    return resolved


def available_runtimes() -> tuple[str, ...]:
    return tuple(sorted(_RUNTIME_FACTORIES))


def runtime_aliases() -> dict[str, str]:
    return dict(_RUNTIME_ALIASES)


def validate_runtime_provider(config: RuntimeConfig | str) -> dict[str, Any]:
    provider = config.provider if isinstance(config, RuntimeConfig) else config
    normalized = normalize_agent_runtime(provider)
    errors: list[str] = []
    if normalized == "claude" and importlib.util.find_spec("claude_agent_sdk") is None:
        errors.append(
            "Claude runtime requires the 'claude-agent-sdk' package to be installed."
        )
    if normalized == "codex" and shutil.which("codex") is None:
        errors.append(
            "Codex runtime requires the 'codex' CLI on PATH."
        )
    return {
        "provider": provider,
        "normalized_provider": normalized,
        "available": not errors,
        "errors": errors,
    }


def create_agent_runtime(
    name: str | None,
    *,
    model: str | None = None,
    session_store=None,
    on_message=None,
    interactive_roles: set[str] | None = None,
) -> AgentRuntime:
    runtime_name = normalize_agent_runtime(name)
    factory = _RUNTIME_FACTORIES.get(runtime_name)
    if factory is None:
        supported = ", ".join(available_runtimes()) or "none"
        raise ValueError(
            f"Unsupported runtime '{runtime_name}'. Supported values: {supported}"
        )
    context = RuntimeContext(
        session_store=session_store,
        on_message=on_message,
        interactive_roles=set(interactive_roles or set()),
    )
    return factory(
        RuntimeConfig(provider=runtime_name, model=model or _default_model_for(runtime_name)),
        context,
    )


def build_runtime(
    config: RuntimeConfig,
    *,
    session_store=None,
    on_message=None,
    interactive_roles: set[str] | None = None,
) -> AgentRuntime:
    return create_agent_runtime(
        config.provider,
        model=config.model,
        session_store=session_store,
        on_message=on_message,
        interactive_roles=interactive_roles,
    )


register_runtime(
    "scripted",
    lambda config, _context: ScriptedAgentRuntime(model=config.model),
)
register_runtime(
    "claude",
    lambda config, context: ClaudeAgentRuntime(
        model=config.model,
        session_store=context.session_store,
        on_message=context.on_message,
        interactive_roles=context.interactive_roles,
    ),
)
register_runtime(
    "codex",
    lambda config, context: CodexAgentRuntime(
        model=config.model,
        session_store=context.session_store,
        on_message=context.on_message,
        interactive_roles=context.interactive_roles,
    ),
)


__all__ = [
    "ClaudeAgentRuntime",
    "CodexAgentRuntime",
    "ScriptedAgentRuntime",
    "available_runtimes",
    "build_runtime",
    "create_agent_runtime",
    "normalize_agent_runtime",
    "register_runtime",
    "runtime_aliases",
    "validate_runtime_provider",
]
