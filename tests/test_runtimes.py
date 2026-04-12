from __future__ import annotations

import sys
import types
import asyncio

import pytest

from mafia.compose_compat import AgentSession, Role
from mafia.config import RuntimeConfig
from mafia.messages import GeneratorReply, SchedulerReply
from mafia.runtime_support import InMemorySessionStore, build_workspace
from mafia.runtimes import build_runtime, normalize_agent_runtime, validate_runtime_provider
from mafia.runtimes.claude import ClaudeAgentRuntime
from mafia.runtimes.codex import CodexAgentRuntime


def test_normalize_agent_runtime_supports_aliases():
    assert normalize_agent_runtime("anthropic") == "claude"
    assert normalize_agent_runtime("openai") == "codex"
    assert normalize_agent_runtime("scripted") == "scripted"


def test_validate_runtime_provider_reports_unknown_provider():
    with pytest.raises(ValueError, match="Unsupported runtime"):
        validate_runtime_provider("mystery")


def test_build_runtime_passes_concurrency_to_claude(monkeypatch):
    class FakeOptions:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.resume = None

    fake_sdk = types.ModuleType("claude_agent_sdk")
    fake_sdk.ClaudeSDKClient = object
    fake_sdk.ClaudeAgentOptions = FakeOptions
    fake_types = types.ModuleType("claude_agent_sdk.types")
    fake_types.ResultMessage = object
    fake_types.PermissionResultAllow = object
    fake_types.PermissionResultDeny = object
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", fake_sdk)
    monkeypatch.setitem(sys.modules, "claude_agent_sdk.types", fake_types)

    runtime = build_runtime(
        RuntimeConfig(
            provider="claude",
            model="claude-test",
            max_concurrency=1,
        ),
        session_store=InMemorySessionStore(),
    )

    assert isinstance(runtime, ClaudeAgentRuntime)
    assert runtime._semaphore is not None  # noqa: SLF001


@pytest.mark.asyncio
async def test_claude_runtime_invokes_with_workspace_and_saves_session(monkeypatch):
    class FakeOptions:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.resume = None

    class FakeResultMessage:
        def __init__(self):
            self.result = '{"decision":"wait","reason":"sdk"}'
            self.structured_output = {"decision": "wait", "reason": "sdk"}
            self.session_id = "claude-session-1"

    class PermissionResultAllow:
        pass

    class PermissionResultDeny:
        def __init__(self, *, message: str):
            self.message = message

    class FakeClient:
        last_options = None
        last_prompt = None

        def __init__(self, *, options):
            FakeClient.last_options = options
            self._options = options

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def query(self, prompt: str):
            FakeClient.last_prompt = prompt

        async def receive_response(self):
            yield FakeResultMessage()

    fake_sdk = types.ModuleType("claude_agent_sdk")
    fake_sdk.ClaudeSDKClient = FakeClient
    fake_sdk.ClaudeAgentOptions = FakeOptions
    fake_types = types.ModuleType("claude_agent_sdk.types")
    fake_types.ResultMessage = FakeResultMessage
    fake_types.PermissionResultAllow = PermissionResultAllow
    fake_types.PermissionResultDeny = PermissionResultDeny
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", fake_sdk)
    monkeypatch.setitem(sys.modules, "claude_agent_sdk.types", fake_types)

    store = InMemorySessionStore()
    workspace = build_workspace("claude-runtime-test")
    runtime = ClaudeAgentRuntime(model="claude-test", session_store=store)
    role = Role(name="scheduler", prompt="Decide whether to send.", metadata={"worker_kind": "scheduler"})

    result = await runtime.invoke(
        role,
        "prompt body",
        output_type=SchedulerReply,
        workspace=workspace,
        session_key="scheduler:test-run",
    )

    session = await store.load("scheduler:test-run")
    assert result.decision == "wait"
    assert session is not None
    assert session.session_id == "claude-session-1"
    assert FakeClient.last_options.cwd == str(workspace.path)
    assert FakeClient.last_options.output_format["type"] == "json_schema"
    assert FakeClient.last_prompt == "prompt body"


@pytest.mark.asyncio
async def test_claude_runtime_parses_fenced_json(monkeypatch):
    class FakeOptions:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.resume = None

    class FakeResultMessage:
        def __init__(self):
            self.result = '```json\\n{"text":"Nice, I could go with that."}\\n```'
            self.structured_output = None
            self.session_id = "claude-session-2"

    class PermissionResultAllow:
        pass

    class PermissionResultDeny:
        def __init__(self, *, message: str):
            self.message = message

    class FakeClient:
        def __init__(self, *, options):
            self._options = options

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def query(self, prompt: str):
            return None

        async def receive_response(self):
            yield FakeResultMessage()

    fake_sdk = types.ModuleType("claude_agent_sdk")
    fake_sdk.ClaudeSDKClient = FakeClient
    fake_sdk.ClaudeAgentOptions = FakeOptions
    fake_types = types.ModuleType("claude_agent_sdk.types")
    fake_types.ResultMessage = FakeResultMessage
    fake_types.PermissionResultAllow = PermissionResultAllow
    fake_types.PermissionResultDeny = PermissionResultDeny
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", fake_sdk)
    monkeypatch.setitem(sys.modules, "claude_agent_sdk.types", fake_types)

    runtime = ClaudeAgentRuntime(model="claude-test", session_store=InMemorySessionStore())
    role = Role(name="generator", prompt="Generate one message.", metadata={"worker_kind": "generator"})
    workspace = build_workspace("claude-fenced-json")

    result = await runtime.invoke(
        role,
        "prompt body",
        output_type=GeneratorReply,
        workspace=workspace,
        session_key="generator:test-run",
    )

    assert result.text == "Nice, I could go with that."


@pytest.mark.asyncio
async def test_claude_runtime_allows_slow_calls_to_finish(monkeypatch):
    class FakeOptions:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.resume = None

    class FakeResultMessage:
        def __init__(self):
            self.result = '{"text":"finally"}'
            self.structured_output = {"text": "finally"}
            self.session_id = "claude-session-slow"

    class PermissionResultAllow:
        pass

    class PermissionResultDeny:
        def __init__(self, *, message: str):
            self.message = message

    class FakeClient:
        def __init__(self, *, options):
            self._options = options

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def query(self, prompt: str):
            return None

        async def receive_response(self):
            await asyncio.sleep(0.02)
            yield FakeResultMessage()

    fake_sdk = types.ModuleType("claude_agent_sdk")
    fake_sdk.ClaudeSDKClient = FakeClient
    fake_sdk.ClaudeAgentOptions = FakeOptions
    fake_types = types.ModuleType("claude_agent_sdk.types")
    fake_types.ResultMessage = FakeResultMessage
    fake_types.PermissionResultAllow = PermissionResultAllow
    fake_types.PermissionResultDeny = PermissionResultDeny
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", fake_sdk)
    monkeypatch.setitem(sys.modules, "claude_agent_sdk.types", fake_types)

    runtime = ClaudeAgentRuntime(model="claude-test", session_store=InMemorySessionStore())
    role = Role(name="generator", prompt="Generate one message.", metadata={"worker_kind": "generator"})
    workspace = build_workspace("claude-slow")

    result = await runtime.invoke(
        role,
        "prompt body",
        output_type=GeneratorReply,
        workspace=workspace,
        session_key="generator:slow-run",
    )

    assert result.text == "finally"


@pytest.mark.asyncio
async def test_claude_runtime_fails_fast_on_usage_limit(monkeypatch):
    class FakeOptions:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.resume = None

    class FakeResultMessage:
        pass

    class PermissionResultAllow:
        pass

    class PermissionResultDeny:
        def __init__(self, *, message: str):
            self.message = message

    class TextBlock:
        def __init__(self, text: str):
            self.text = text

    class AssistantMessage:
        def __init__(self, text: str):
            self.content = [TextBlock(text)]

    class FakeClient:
        def __init__(self, *, options):
            self._options = options

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def query(self, prompt: str):
            return None

        async def receive_response(self):
            yield AssistantMessage("You've hit your limit · resets 10pm (America/Los_Angeles)")
            if False:
                yield FakeResultMessage()

    fake_sdk = types.ModuleType("claude_agent_sdk")
    fake_sdk.ClaudeSDKClient = FakeClient
    fake_sdk.ClaudeAgentOptions = FakeOptions
    fake_types = types.ModuleType("claude_agent_sdk.types")
    fake_types.ResultMessage = FakeResultMessage
    fake_types.PermissionResultAllow = PermissionResultAllow
    fake_types.PermissionResultDeny = PermissionResultDeny
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", fake_sdk)
    monkeypatch.setitem(sys.modules, "claude_agent_sdk.types", fake_types)

    runtime = ClaudeAgentRuntime(model="claude-test", session_store=InMemorySessionStore())
    role = Role(name="generator", prompt="Generate one message.", metadata={"worker_kind": "generator"})
    workspace = build_workspace("claude-limit")

    with pytest.raises(RuntimeError, match="hit your limit"):
        await runtime.invoke(
            role,
            "prompt body",
            output_type=GeneratorReply,
            workspace=workspace,
            session_key="generator:limit-run",
        )


@pytest.mark.asyncio
async def test_claude_runtime_allows_concurrent_invokes_by_default(monkeypatch):
    class FakeOptions:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.resume = None

    class FakeResultMessage:
        def __init__(self):
            self.result = '{"text":"hello"}'
            self.structured_output = {"text": "hello"}
            self.session_id = "claude-session-parallel"

    class PermissionResultAllow:
        pass

    class PermissionResultDeny:
        def __init__(self, *, message: str):
            self.message = message

    class FakeClient:
        active = 0
        max_active = 0

        def __init__(self, *, options):
            self._options = options

        async def __aenter__(self):
            FakeClient.active += 1
            FakeClient.max_active = max(FakeClient.max_active, FakeClient.active)
            return self

        async def __aexit__(self, exc_type, exc, tb):
            FakeClient.active -= 1
            return None

        async def query(self, prompt: str):
            return None

        async def receive_response(self):
            await asyncio.sleep(0.02)
            yield FakeResultMessage()

    fake_sdk = types.ModuleType("claude_agent_sdk")
    fake_sdk.ClaudeSDKClient = FakeClient
    fake_sdk.ClaudeAgentOptions = FakeOptions
    fake_types = types.ModuleType("claude_agent_sdk.types")
    fake_types.ResultMessage = FakeResultMessage
    fake_types.PermissionResultAllow = PermissionResultAllow
    fake_types.PermissionResultDeny = PermissionResultDeny
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", fake_sdk)
    monkeypatch.setitem(sys.modules, "claude_agent_sdk.types", fake_types)

    runtime = ClaudeAgentRuntime(model="claude-test", session_store=InMemorySessionStore())
    role = Role(name="generator", prompt="Generate one message.", metadata={"worker_kind": "generator"})
    workspace = build_workspace("claude-parallel")

    await asyncio.gather(
        runtime.invoke(role, "prompt one", output_type=GeneratorReply, workspace=workspace, session_key="generator:one"),
        runtime.invoke(role, "prompt two", output_type=GeneratorReply, workspace=workspace, session_key="generator:two"),
    )

    assert FakeClient.max_active >= 2


@pytest.mark.asyncio
async def test_claude_runtime_can_optionally_limit_concurrency(monkeypatch):
    class FakeOptions:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.resume = None

    class FakeResultMessage:
        def __init__(self):
            self.result = '{"text":"hello"}'
            self.structured_output = {"text": "hello"}
            self.session_id = "claude-session-serial"

    class PermissionResultAllow:
        pass

    class PermissionResultDeny:
        def __init__(self, *, message: str):
            self.message = message

    class FakeClient:
        active = 0
        max_active = 0

        def __init__(self, *, options):
            self._options = options

        async def __aenter__(self):
            FakeClient.active += 1
            FakeClient.max_active = max(FakeClient.max_active, FakeClient.active)
            return self

        async def __aexit__(self, exc_type, exc, tb):
            FakeClient.active -= 1
            return None

        async def query(self, prompt: str):
            return None

        async def receive_response(self):
            await asyncio.sleep(0.02)
            yield FakeResultMessage()

    fake_sdk = types.ModuleType("claude_agent_sdk")
    fake_sdk.ClaudeSDKClient = FakeClient
    fake_sdk.ClaudeAgentOptions = FakeOptions
    fake_types = types.ModuleType("claude_agent_sdk.types")
    fake_types.ResultMessage = FakeResultMessage
    fake_types.PermissionResultAllow = PermissionResultAllow
    fake_types.PermissionResultDeny = PermissionResultDeny
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", fake_sdk)
    monkeypatch.setitem(sys.modules, "claude_agent_sdk.types", fake_types)

    runtime = ClaudeAgentRuntime(model="claude-test", session_store=InMemorySessionStore(), max_concurrency=1)
    role = Role(name="generator", prompt="Generate one message.", metadata={"worker_kind": "generator"})
    workspace = build_workspace("claude-serial")

    await asyncio.gather(
        runtime.invoke(role, "prompt one", output_type=GeneratorReply, workspace=workspace, session_key="generator:one"),
        runtime.invoke(role, "prompt two", output_type=GeneratorReply, workspace=workspace, session_key="generator:two"),
    )

    assert FakeClient.max_active == 1


@pytest.mark.asyncio
async def test_claude_runtime_one_shot_roles_do_not_resume_or_save_sessions(monkeypatch):
    class FakeOptions:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)
            self.resume = None

    class FakeResultMessage:
        def __init__(self):
            self.result = '{"text":"hello"}'
            self.structured_output = {"text": "hello"}
            self.session_id = "claude-session-one-shot"

    class PermissionResultAllow:
        pass

    class PermissionResultDeny:
        def __init__(self, *, message: str):
            self.message = message

    class FakeClient:
        last_options = None

        def __init__(self, *, options):
            FakeClient.last_options = options

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return None

        async def query(self, prompt: str):
            return None

        async def receive_response(self):
            yield FakeResultMessage()

    fake_sdk = types.ModuleType("claude_agent_sdk")
    fake_sdk.ClaudeSDKClient = FakeClient
    fake_sdk.ClaudeAgentOptions = FakeOptions
    fake_types = types.ModuleType("claude_agent_sdk.types")
    fake_types.ResultMessage = FakeResultMessage
    fake_types.PermissionResultAllow = PermissionResultAllow
    fake_types.PermissionResultDeny = PermissionResultDeny
    monkeypatch.setitem(sys.modules, "claude_agent_sdk", fake_sdk)
    monkeypatch.setitem(sys.modules, "claude_agent_sdk.types", fake_types)

    store = InMemorySessionStore()
    await store.save(
        AgentSession(
            session_key="generator:one-shot",
            session_id="prior-session",
            metadata={"turns": [{"role": "assistant", "text": "old"}]},
        )
    )
    runtime = ClaudeAgentRuntime(model="claude-test", session_store=store)
    role = Role(
        name="generator",
        prompt="Generate one message.",
        metadata={"worker_kind": "generator", "one_shot": True},
    )
    workspace = build_workspace("claude-one-shot")

    await runtime.invoke(
        role,
        "prompt body",
        output_type=GeneratorReply,
        workspace=workspace,
        session_key="generator:one-shot",
    )

    session = await store.load("generator:one-shot")
    assert FakeClient.last_options.resume is None
    assert session is not None
    assert session.session_id == "prior-session"
    assert session.metadata["turns"] == [{"role": "assistant", "text": "old"}]


@pytest.mark.asyncio
async def test_codex_runtime_invokes_with_session_context(monkeypatch):
    monkeypatch.setattr("shutil.which", lambda _name: "/tmp/codex")
    store = InMemorySessionStore()
    await store.save(
        AgentSession(
            session_key="scheduler:test-run",
            metadata={"turns": [{"role": "assistant", "text": "We were talking about lunch."}]},
        )
    )
    runtime = CodexAgentRuntime(model="gpt-5-test", session_store=store)
    captured: dict[str, str] = {}

    async def fake_run_codex(role, prompt: str, *, workspace, output_type):
        captured["prompt"] = prompt
        captured["workspace"] = str(workspace.path)
        return '{"decision":"send","reason":"codex"}'

    monkeypatch.setattr(runtime, "_run_codex", fake_run_codex)
    workspace = build_workspace("codex-runtime-test")
    role = Role(name="scheduler", prompt="Decide whether to send.", metadata={"worker_kind": "scheduler"})

    result = await runtime.invoke(
        role,
        "next prompt",
        output_type=SchedulerReply,
        workspace=workspace,
        session_key="scheduler:test-run",
    )

    session = await store.load("scheduler:test-run")
    assert result.decision == "send"
    assert session is not None
    assert "We were talking about lunch." in captured["prompt"]
    assert captured["workspace"] == str(workspace.path)
