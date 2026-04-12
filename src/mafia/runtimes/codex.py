from __future__ import annotations

import asyncio
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from mafia.compose_compat import AgentRuntime, AgentSession, SessionStore, Workspace


def _prepare_schema(schema: dict[str, Any]) -> dict[str, Any]:
    defs = schema.pop("$defs", None)

    def _resolve(obj: Any) -> Any:
        if isinstance(obj, dict):
            ref = obj.get("$ref")
            if ref and isinstance(ref, str) and defs:
                name = ref.rsplit("/", 1)[-1]
                if name in defs:
                    return _resolve(defs[name])
            resolved = {key: _resolve(value) for key, value in obj.items()}
            if resolved.get("type") == "object" and "additionalProperties" not in resolved:
                resolved["additionalProperties"] = False
            properties = resolved.get("properties")
            if isinstance(properties, dict):
                resolved["required"] = list(properties.keys())
            return resolved
        if isinstance(obj, list):
            return [_resolve(item) for item in obj]
        return obj

    return _resolve(schema)


class CodexAgentRuntime(AgentRuntime):
    """Trimmed Codex CLI runtime adapted from iriai-build-v2 for chat-agent use."""

    name = "codex"

    def __init__(
        self,
        *,
        model: str = "gpt-5",
        session_store: SessionStore | None = None,
        on_message: Any | None = None,
        interactive_roles: set[str] | None = None,
        codex_command: str = "codex",
    ) -> None:
        if shutil.which(codex_command) is None:
            raise ImportError(
                "CodexAgentRuntime requires the Codex CLI on PATH. "
                "Install it with: npm install -g @openai/codex"
            )
        self._default_model = model
        self.session_store = session_store
        self.on_message = on_message
        self._interactive_roles = interactive_roles or set()
        self._codex_command = codex_command

    async def invoke(
        self,
        role,
        prompt: str,
        *,
        output_type: type[BaseModel] | None = None,
        workspace: Workspace | None = None,
        session_key: str | None = None,
    ) -> str | BaseModel:
        session: AgentSession | None = None
        if session_key and self.session_store is not None:
            session = await self.session_store.load(session_key)

        effective_prompt = self._compose_prompt(
            role,
            prompt,
            session=session,
            output_type=output_type,
        )
        final_text = await self._run_codex(
            role,
            effective_prompt,
            workspace=workspace,
            output_type=output_type,
        )

        if session_key and self.session_store is not None:
            current = session or AgentSession(session_key=session_key)
            turns = list(current.metadata.get("turns", []))
            turns.append({"role": "user", "text": prompt, "turn": len(turns) + 1})
            turns.append({"role": "assistant", "text": final_text, "turn": len(turns) + 1})
            current.metadata["turns"] = turns
            await self.session_store.save(current)

        if output_type is None:
            return final_text

        max_retries = 2
        last_error: Exception | None = None
        current_text = final_text
        for attempt in range(max_retries + 1):
            try:
                payload = json.loads(current_text)
                return output_type.model_validate(payload)
            except (json.JSONDecodeError, Exception) as exc:
                last_error = exc
                if attempt >= max_retries:
                    break
                current_text = await self._run_codex(
                    role,
                    (
                        f"Your previous response was not valid JSON for {output_type.__name__}. "
                        f"Error: {exc}\n\n"
                        "Please output ONLY valid JSON matching the schema.\n\n"
                        f"Previous response:\n{current_text}"
                    ),
                    workspace=workspace,
                    output_type=output_type,
                )
        raise RuntimeError(
            f"Codex failed to return valid JSON for {output_type.__name__}: {last_error}"
        )

    def _compose_prompt(
        self,
        role,
        prompt: str,
        *,
        session: AgentSession | None,
        output_type: type[BaseModel] | None,
    ) -> str:
        sections = [
            f"## Role\nName: {role.name}",
            f"## Role Instructions\n{role.prompt.strip()}",
        ]
        if getattr(role, "tools", None):
            sections.append(
                "## Available Tooling Expectations\n"
                f"Use Codex tools to cover these intended capabilities when possible: {', '.join(role.tools)}."
            )
        prior = self._fallback_session_context(session)
        if prior:
            sections.append(prior)
        if output_type is not None:
            sections.append(
                f"## Output Contract\nReturn JSON matching the {output_type.__name__} schema."
            )
        sections.append(f"## Current Task\n{prompt}")
        return "\n\n".join(section for section in sections if section.strip())

    def _fallback_session_context(self, session: AgentSession | None) -> str:
        if not session:
            return ""
        turns = session.metadata.get("turns", [])
        if not turns:
            return ""
        recent_turns = turns[-8:]
        rendered: list[str] = []
        for turn in recent_turns:
            who = str(turn.get("role", "assistant")).title()
            text = str(turn.get("text", "")).strip()
            if text:
                rendered.append(f"{who}: {text}")
        if not rendered:
            return ""
        return "## Prior Conversation\n" + "\n\n".join(rendered)

    def _build_command(
        self,
        *,
        role,
        workspace: Workspace | None,
        output_schema_path: str | None,
        output_path: str,
    ) -> list[str]:
        args = [
            self._codex_command,
            "exec",
            "--json",
            "--skip-git-repo-check",
            "--full-auto",
            "-o",
            output_path,
        ]
        model = getattr(role, "model", None) or self._default_model
        if model:
            args.extend(["-m", model])
        if output_schema_path:
            args.extend(["--output-schema", output_schema_path])
        if workspace and workspace.path:
            args.extend(["-C", str(workspace.path)])
        args.append("-")
        return args

    async def _run_codex(
        self,
        role,
        prompt: str,
        *,
        workspace: Workspace | None,
        output_type: type[BaseModel] | None,
    ) -> str:
        temp_dir = None
        if workspace and workspace.path:
            temp_dir = str(Path(workspace.path))

        schema_path: str | None = None
        output_path: str | None = None
        try:
            if output_type is not None:
                with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    suffix=".json",
                    delete=False,
                    dir=temp_dir,
                ) as schema_file:
                    json.dump(_prepare_schema(output_type.model_json_schema()), schema_file)
                    schema_path = schema_file.name
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                suffix=".txt",
                delete=False,
                dir=temp_dir,
            ) as output_file:
                output_path = output_file.name

            command = self._build_command(
                role=role,
                workspace=workspace,
                output_schema_path=schema_path,
                output_path=output_path,
            )
            return await self._run_process(command, prompt, output_path)
        finally:
            for path in (schema_path, output_path):
                if path:
                    try:
                        Path(path).unlink(missing_ok=True)
                    except OSError:
                        pass

    async def _run_process(
        self,
        command: list[str],
        prompt: str,
        output_path: str | None,
    ) -> str:
        try:
            proc = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as exc:  # pragma: no cover - availability check covers this
            raise RuntimeError(
                "Could not start the Codex CLI. Ensure `codex` is installed and on PATH."
            ) from exc

        assert proc.stdin is not None
        assert proc.stdout is not None
        assert proc.stderr is not None

        stdout_task = asyncio.create_task(proc.stdout.read())
        stderr_task = asyncio.create_task(proc.stderr.read())
        proc.stdin.write(prompt.encode("utf-8"))
        await proc.stdin.drain()
        proc.stdin.close()
        return_code = await proc.wait()
        stdout_text = (await stdout_task).decode("utf-8", errors="replace")
        stderr_text = (await stderr_task).decode("utf-8", errors="replace")

        final_text = ""
        if output_path:
            final_text = Path(output_path).read_text(encoding="utf-8").strip()
        if not final_text:
            final_text = self._extract_last_text(stdout_text)
        if return_code != 0:
            details = stderr_text.strip() or stdout_text.strip() or "unknown error"
            raise RuntimeError(f"Codex CLI failed with exit code {return_code}: {details}")
        if not final_text:
            raise RuntimeError("Codex returned no final message")
        if self.on_message is not None:
            self.on_message({"text": final_text})
        return final_text

    def _extract_last_text(self, stdout_text: str) -> str:
        last_text = ""
        for line in stdout_text.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            try:
                payload = json.loads(stripped)
            except json.JSONDecodeError:
                last_text = stripped
                continue
            if isinstance(payload, dict):
                last_text = (
                    payload.get("text")
                    or payload.get("content")
                    or payload.get("output")
                    or last_text
                )
        return str(last_text).strip()


__all__ = ["CodexAgentRuntime"]
