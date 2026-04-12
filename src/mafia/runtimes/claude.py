from __future__ import annotations

import asyncio
import logging
import os
import re
from collections.abc import Callable
from typing import Any

from pydantic import BaseModel

from mafia.compose_compat import AgentRuntime, AgentSession, SessionStore, Workspace

logger = logging.getLogger(__name__)

_WRITE_TOOLS = {"Edit", "Write", "MultiEdit", "NotebookEdit"}
_PATH_PARAMS: dict[str, str] = {
    "Edit": "file_path",
    "Write": "file_path",
    "MultiEdit": "file_path",
    "NotebookEdit": "file_path",
}
_JSON_FENCE_RE = re.compile(r"```(?:json)?\s*(?P<body>[\s\S]*?)\s*```", re.IGNORECASE)
_LIMIT_ERROR_RE = re.compile(
    r"(you(?:'|’)ve hit your limit|usage limit|rate limit|quota)",
    re.IGNORECASE,
)


def _make_write_guard(allowed_dir: str) -> Any:
    from claude_agent_sdk.types import PermissionResultAllow, PermissionResultDeny

    resolved_root = os.path.realpath(allowed_dir)

    async def _guard(
        tool_name: str,
        tool_input: dict[str, Any],
        _context: Any,
    ) -> PermissionResultAllow | PermissionResultDeny:
        if tool_name not in _WRITE_TOOLS:
            return PermissionResultAllow()

        path_key = _PATH_PARAMS.get(tool_name)
        if not path_key:
            return PermissionResultAllow()

        target = tool_input.get(path_key, "")
        if not target:
            return PermissionResultDeny(
                message=f"Write denied: no {path_key} provided",
            )

        resolved = os.path.realpath(target)
        if resolved == resolved_root or resolved.startswith(resolved_root + os.sep):
            return PermissionResultAllow()

        return PermissionResultDeny(
            message=(
                f"Write denied: {target} is outside the allowed workspace "
                f"({allowed_dir}). All file writes must stay within the workspace."
            ),
        )

    return _guard


def _inline_defs(schema: dict[str, Any]) -> dict[str, Any]:
    defs = schema.pop("$defs", None)
    if not defs:
        return schema

    def _resolve(obj: Any) -> Any:
        if isinstance(obj, dict):
            ref = obj.get("$ref")
            if ref and isinstance(ref, str):
                name = ref.rsplit("/", 1)[-1]
                if name in defs:
                    return _resolve(defs[name])
                return obj
            return {key: _resolve(value) for key, value in obj.items()}
        if isinstance(obj, list):
            return [_resolve(item) for item in obj]
        return obj

    return _resolve(schema)


def _message_text(msg: Any) -> str:
    content = getattr(msg, "content", None)
    if not isinstance(content, list):
        return ""
    parts: list[str] = []
    for block in content:
        text = getattr(block, "text", None)
        if isinstance(text, str) and text.strip():
            parts.append(text.strip())
    return "\n".join(parts).strip()


def _terminal_error_from_message(msg: Any) -> str | None:
    text = _message_text(msg)
    if text and _LIMIT_ERROR_RE.search(text):
        return text
    return None


class ClaudeAgentRuntime(AgentRuntime):
    """Trimmed Claude runtime adapted from iriai-build-v2 for chat-agent use."""

    name = "claude"

    def __init__(
        self,
        *,
        model: str = "claude-haiku-4-5-20251001",
        session_store: SessionStore | None = None,
        on_message: Callable[[Any], None] | None = None,
        interactive_roles: set[str] | None = None,
        max_concurrency: int | None = None,
    ) -> None:
        try:
            import claude_agent_sdk  # noqa: F401
        except ImportError as exc:  # pragma: no cover - dependency validation covers this
            raise ImportError(
                "ClaudeAgentRuntime requires the 'claude-agent-sdk' package. "
                "Install it with: pip install claude-agent-sdk"
            ) from exc
        self._default_model = model
        self.session_store = session_store
        self.on_message = on_message
        self._interactive_roles = interactive_roles or set()
        self._semaphore = (
            asyncio.Semaphore(max(1, max_concurrency))
            if max_concurrency is not None
            else None
        )

    async def invoke(
        self,
        role,
        prompt: str,
        *,
        output_type: type[BaseModel] | None = None,
        workspace: Workspace | None = None,
        session_key: str | None = None,
    ) -> str | BaseModel:
        if self._semaphore is None:
            return await self._invoke_once(
                role,
                prompt,
                output_type=output_type,
                workspace=workspace,
                session_key=session_key,
            )
        async with self._semaphore:
            return await self._invoke_once(
                role,
                prompt,
                output_type=output_type,
                workspace=workspace,
                session_key=session_key,
            )

    async def _invoke_once(
        self,
        role,
        prompt: str,
        *,
        output_type: type[BaseModel] | None = None,
        workspace: Workspace | None = None,
        session_key: str | None = None,
    ) -> str | BaseModel:
        from claude_agent_sdk import ClaudeSDKClient
        from claude_agent_sdk.types import ResultMessage

        options = self._build_options(role, workspace, output_type)
        one_shot = bool(role.metadata.get("one_shot", False))

        session: AgentSession | None = None
        if session_key and self.session_store is not None and not one_shot:
            session = await self.session_store.load(session_key)
            if session and session.session_id:
                options.resume = session.session_id

        result_msg = None
        async with ClaudeSDKClient(options=options) as client:
            await client.query(prompt)
            async for msg in client.receive_response():
                if self.on_message is not None:
                    self.on_message(msg)
                terminal_error = _terminal_error_from_message(msg)
                if terminal_error:
                    raise RuntimeError(f"Claude runtime unavailable: {terminal_error}")
                if isinstance(msg, ResultMessage):
                    result_msg = msg

        if result_msg is None:
            raise RuntimeError("Claude query completed without a result message")

        result_text = getattr(result_msg, "result", "") or ""
        session_id = getattr(result_msg, "session_id", None)
        if session_key and self.session_store is not None and not one_shot:
            current = session or AgentSession(session_key=session_key)
            current.session_id = session_id
            turns = list(current.metadata.get("turns", []))
            turns.append({"role": "user", "text": prompt, "turn": len(turns) + 1})
            turns.append({"role": "assistant", "text": result_text, "turn": len(turns) + 1})
            current.metadata["turns"] = turns
            await self.session_store.save(current)

        if output_type is None:
            return result_text

        structured = getattr(result_msg, "structured_output", None)
        if structured is not None:
            return output_type.model_validate(structured)
        return self._validate_structured_output(output_type, result_text)

    def _build_options(
        self,
        role,
        workspace: Workspace | None,
        output_type: type[BaseModel] | None = None,
    ) -> Any:
        from claude_agent_sdk import ClaudeAgentOptions

        cwd = str(workspace.path) if workspace else None
        write_guard = None
        sandbox = None
        if cwd and role.metadata.get("sandbox", True):
            write_guard = _make_write_guard(cwd)
            sandbox = {"enabled": True}

        options = ClaudeAgentOptions(
            system_prompt=role.prompt,
            allowed_tools=getattr(role, "tools", []),
            model=getattr(role, "model", None) or self._default_model,
            cwd=cwd,
            permission_mode="bypassPermissions",
            effort=getattr(role, "effort", None) if getattr(role, "effort", None) is not None else "high",
            max_buffer_size=50 * 1024 * 1024,
            sandbox=sandbox,
            can_use_tool=write_guard,
        )

        if output_type is not None:
            options.output_format = {
                "type": "json_schema",
                "schema": _inline_defs(output_type.model_json_schema()),
            }

        return options

    def _validate_structured_output(
        self,
        output_type: type[BaseModel],
        result_text: str,
    ) -> BaseModel:
        for candidate in self._structured_output_candidates(result_text):
            try:
                return output_type.model_validate_json(candidate)
            except Exception:
                continue
        return output_type.model_validate_json(result_text)

    def _structured_output_candidates(self, result_text: str) -> list[str]:
        candidates: list[str] = []
        stripped = result_text.strip()
        if stripped:
            candidates.append(stripped)
        fenced = _JSON_FENCE_RE.search(result_text)
        if fenced:
            body = fenced.group("body").strip()
            if body:
                candidates.append(body)
        for opener, closer in (("{", "}"), ("[", "]")):
            start = result_text.find(opener)
            end = result_text.rfind(closer)
            if start != -1 and end != -1 and end > start:
                snippet = result_text[start : end + 1].strip()
                if snippet:
                    candidates.append(snippet)
        deduped: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            deduped.append(candidate)
        return deduped


__all__ = ["ClaudeAgentRuntime"]
