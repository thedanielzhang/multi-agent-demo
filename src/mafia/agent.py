from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from pydantic import BaseModel

from mafia.compose_compat import AgentActor, AgentRuntime, Role, Workspace
from mafia.config import AgentConfig


@dataclass
class AgentActors:
    generator: AgentActor
    scheduler: AgentActor
    analyzer: AgentActor
    voter: AgentActor


def build_agent_actors(agent: AgentConfig) -> AgentActors:
    common = {
        "goals": list(agent.goals),
        "style_prompt": agent.style_prompt,
        "max_words": agent.max_words,
        "talkativeness": agent.personality.talkativeness,
        "confidence": agent.personality.confidence,
        "reactivity": agent.personality.reactivity,
        "topic_loyalty": agent.personality.topic_loyalty,
        "agent_id": agent.id,
    }
    generator = AgentActor(
        name=f"{agent.id}.generator",
        role=Role(
            name="generator",
            prompt="Generate one message.",
            effort="low",
            metadata={**common, "worker_kind": "generator", "one_shot": True},
        ),
    )
    scheduler = AgentActor(
        name=f"{agent.id}.scheduler",
        role=Role(
            name="scheduler",
            prompt="Decide whether to send or wait.",
            effort="low",
            metadata={**common, "worker_kind": "scheduler", "one_shot": True},
        ),
    )
    analyzer = AgentActor(
        name=f"{agent.id}.analyzer",
        role=Role(
            name="analyzer",
            prompt="Analyze conversation topics.",
            effort="low",
            metadata={**common, "worker_kind": "analyzer", "one_shot": True},
        ),
    )
    voter = AgentActor(
        name=f"{agent.id}.voter",
        role=Role(
            name="voter",
            prompt="Choose a mafia vote target.",
            effort="low",
            metadata={**common, "worker_kind": "voter", "one_shot": True},
        ),
    )
    return AgentActors(generator=generator, scheduler=scheduler, analyzer=analyzer, voter=voter)


class AgentInvoker:
    """Small helper that uses the upstream AgentRuntime.invoke boundary directly."""

    def __init__(self, runtime: AgentRuntime, run_id: str, workspace: Workspace) -> None:
        self._runtime = runtime
        self._run_id = run_id
        self._workspace = workspace

    def session_key_for(self, actor: AgentActor) -> str:
        agent_id = actor.role.metadata.get("agent_id", "agent")
        worker_kind = actor.role.metadata.get("worker_kind", actor.role.name)
        return f"run:{self._run_id}:participant:{agent_id}:worker:{worker_kind}"

    def workspace_for(self, actor: AgentActor) -> Workspace:
        agent_id = actor.role.metadata.get("agent_id", "agent")
        worker_kind = actor.role.metadata.get("worker_kind", actor.role.name)
        path = self._workspace.path / "participants" / str(agent_id) / str(worker_kind)
        path.mkdir(parents=True, exist_ok=True)
        return Workspace(
            id=f"{self._workspace.id}:{agent_id}:{worker_kind}",
            path=path,
            branch=self._workspace.branch,
            metadata={
                **dict(self._workspace.metadata),
                "agent_id": agent_id,
                "worker_kind": worker_kind,
            },
        )

    async def invoke(
        self,
        actor: AgentActor,
        *,
        prompt: str,
        input_data: BaseModel | None = None,
        output_type: type[BaseModel] | None = None,
    ) -> Any:
        session_key = self.session_key_for(actor)
        workspace = self.workspace_for(actor)
        result = await self._runtime.invoke(
            actor.role,
            self._compose_prompt(prompt, input_data),
            output_type=output_type,
            session_key=session_key,
            workspace=workspace,
        )
        if output_type is None:
            return result
        if isinstance(result, output_type):
            return result
        if isinstance(result, dict):
            return output_type.model_validate(result)
        if isinstance(result, str):
            return output_type.model_validate_json(result)
        raise TypeError(f"unexpected result for {actor.name}: {type(result)!r}")

    def _compose_prompt(self, prompt: str, input_data: BaseModel | None) -> str:
        if input_data is None:
            return prompt
        payload = self._sanitize_input_payload(input_data.model_dump(mode="json"))
        return f"{prompt}\n\nINPUT_JSON:\n{json.dumps(payload, indent=2)}"

    def _sanitize_input_payload(self, value: Any) -> Any:
        if isinstance(value, dict):
            cleaned: dict[str, Any] = {}
            for key, item in value.items():
                if key == "is_human":
                    continue
                if key == "sender_kind":
                    if item in {"human", "agent"}:
                        cleaned[key] = "participant"
                    else:
                        cleaned[key] = item
                    continue
                cleaned[key] = self._sanitize_input_payload(item)
            return cleaned
        if isinstance(value, list):
            return [self._sanitize_input_payload(item) for item in value]
        return value
