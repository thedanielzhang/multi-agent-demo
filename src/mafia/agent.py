from __future__ import annotations

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
            metadata={**common, "worker_kind": "generator"},
        ),
    )
    scheduler = AgentActor(
        name=f"{agent.id}.scheduler",
        role=Role(
            name="scheduler",
            prompt="Decide whether to send or wait.",
            metadata={**common, "worker_kind": "scheduler"},
        ),
    )
    analyzer = AgentActor(
        name=f"{agent.id}.analyzer",
        role=Role(
            name="analyzer",
            prompt="Analyze conversation topics.",
            metadata={**common, "worker_kind": "analyzer"},
        ),
    )
    return AgentActors(generator=generator, scheduler=scheduler, analyzer=analyzer)


class AgentInvoker:
    """Small helper that uses the upstream AgentRuntime.invoke boundary directly."""

    def __init__(self, runtime: AgentRuntime, run_id: str, workspace: Workspace) -> None:
        self._runtime = runtime
        self._run_id = run_id
        self._workspace = workspace

    async def invoke(
        self,
        actor: AgentActor,
        *,
        prompt: str,
        input_data: BaseModel | None = None,
        output_type: type[BaseModel] | None = None,
    ) -> Any:
        result = await self._runtime.invoke(
            actor.role,
            self._compose_prompt(prompt, input_data),
            output_type=output_type,
            session_key=f"{actor.name}:{self._run_id}",
            workspace=self._workspace,
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
        return f"{prompt}\n\nINPUT_JSON:\n{input_data.model_dump_json(indent=2)}"
