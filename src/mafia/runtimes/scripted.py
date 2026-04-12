from __future__ import annotations

import json
import re
from typing import Any

from mafia.compose_compat import AgentRuntime
from mafia.messages import (
    AnalyzerInputSnapshot,
    MafiaVoteInputSnapshot,
    SchedulerInputSnapshot,
    GeneratorInputSnapshot,
)
from mafia.scripted_logic import ScriptedAgentLogic


class ScriptedAgentRuntime(AgentRuntime):
    """Deterministic runtime used for tests and local development."""

    name = "scripted"

    def __init__(self, *, model: str = "mock-model", logic: ScriptedAgentLogic | None = None) -> None:
        self._model = model
        self._logic = logic or ScriptedAgentLogic()

    async def invoke(
        self,
        role,
        prompt: str,
        *,
        output_type=None,
        workspace=None,
        session_key=None,
    ) -> Any:
        del output_type, workspace, session_key
        worker_kind = role.metadata.get("worker_kind")
        payload = self._parse_input(prompt)
        if worker_kind == "scheduler":
            snapshot = SchedulerInputSnapshot.model_validate(payload)
            return self._logic.scheduler_reply(snapshot, role.metadata)
        if worker_kind == "generator":
            snapshot = GeneratorInputSnapshot.model_validate(payload)
            return self._logic.generator_reply(snapshot, role.metadata)
        if worker_kind == "analyzer":
            snapshot = AnalyzerInputSnapshot.model_validate(payload)
            return self._logic.analyzer_reply(snapshot)
        if worker_kind == "voter":
            snapshot = MafiaVoteInputSnapshot.model_validate(payload)
            return self._logic.mafia_vote_reply(snapshot, role.metadata)
        return ""

    def _parse_input(self, prompt: str) -> dict[str, Any]:
        match = re.search(r"INPUT_JSON:\n(?P<payload>\{.*\})\s*$", prompt, re.DOTALL)
        if not match:
            return {}
        return json.loads(match.group("payload"))
