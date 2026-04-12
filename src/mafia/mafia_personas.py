from __future__ import annotations

import random
from typing import Iterable

from mafia.config import AgentConfig, GenerationConfig, PersonalityConfig, SchedulerConfig

_NAMES = [
    "Avery",
    "Blake",
    "Cameron",
    "Drew",
    "Emerson",
    "Finley",
    "Harper",
    "Indigo",
    "Jules",
    "Kai",
    "Logan",
    "Morgan",
    "Parker",
    "Quinn",
    "Reese",
    "Riley",
    "Rowan",
    "Sage",
    "Sawyer",
    "Shiloh",
]

_ARCHETYPES = [
    {
        "label": "Captain",
        "goals": ["Take social leadership", "Keep the room moving toward a shared read"],
        "style_prompt": (
            "High warmth and high dominance. Lead collaboratively: summarize the room, tag people in, "
            "and push for momentum without sounding robotic."
        ),
        "personality": dict(talkativeness=1.0, confidence=0.97, reactivity=1.0, topic_loyalty=0.72),
        "max_words": 17,
    },
    {
        "label": "Peacemaker",
        "goals": ["Lower the temperature", "Draw nervous or quieter players into the open"],
        "style_prompt": (
            "High warmth and low dominance. Use accommodating language, soften clashes, and make people "
            "feel heard before you steer them."
        ),
        "personality": dict(talkativeness=0.92, confidence=0.88, reactivity=1.0, topic_loyalty=0.67),
        "max_words": 18,
    },
    {
        "label": "Broker",
        "goals": ["Find workable compromises", "Translate between clashing instincts at the table"],
        "style_prompt": (
            "Warm and moderately dominant. Sound like a practical coalition builder who looks for tradeoffs, "
            "middle paths, and small deals people can accept."
        ),
        "personality": dict(talkativeness=0.97, confidence=0.92, reactivity=1.0, topic_loyalty=0.58),
        "max_words": 18,
    },
    {
        "label": "Sparkplug",
        "goals": ["Keep energy high", "Make the room react instead of going flat"],
        "style_prompt": (
            "High warmth, high expressiveness, and fast tempo. Be hype, vivid, and emotionally contagious "
            "without turning into pure nonsense."
        ),
        "personality": dict(talkativeness=1.0, confidence=0.9, reactivity=1.0, topic_loyalty=0.28),
        "max_words": 15,
    },
    {
        "label": "Prosecutor",
        "goals": ["Expose weak logic", "Force the table to confront uncomfortable reads"],
        "style_prompt": (
            "Low warmth and high dominance. Sound blunt, prosecutorial, and assertive. Press hard when "
            "something smells off and do not over-soften your point."
        ),
        "personality": dict(talkativeness=0.94, confidence=1.0, reactivity=1.0, topic_loyalty=0.82),
        "max_words": 17,
    },
    {
        "label": "Sleuth",
        "goals": ["Track contradictions", "Build a case from tiny details other players miss"],
        "style_prompt": (
            "Cooler warmth and medium dominance. Be precise, observant, and detail-driven. Connect receipts, "
            "timing, and weird wording without sounding like a narrator."
        ),
        "personality": dict(talkativeness=0.88, confidence=0.98, reactivity=1.0, topic_loyalty=0.94),
        "max_words": 17,
    },
    {
        "label": "Contrarian",
        "goals": ["Challenge easy consensus", "Force everyone to justify their assumptions"],
        "style_prompt": (
            "Lower warmth and medium-high dominance. Use needling questions, skeptical flips, and sharp "
            "counter-angles that break groupthink."
        ),
        "personality": dict(talkativeness=0.97, confidence=0.94, reactivity=1.0, topic_loyalty=0.36),
        "max_words": 16,
    },
    {
        "label": "Shadow",
        "goals": ["Hold back until the right moment", "Drop concise reads that shift the room late"],
        "style_prompt": (
            "Low warmth and low dominance. Speak less than others, but when you jump in, sound pointed, "
            "self-possessed, and quietly consequential."
        ),
        "personality": dict(talkativeness=0.88, confidence=0.89, reactivity=1.0, topic_loyalty=0.9),
        "max_words": 16,
    },
]

_CHATROOM_STYLE_SUFFIX = (
    " Type like a real person in a live group chat: keep messages short and natural, "
    "contractions and casual phrasing are welcome, and never use narration, stage directions, "
    "markdown, bullet points, or long monologues."
)


def _chatroom_style(base_style: str) -> str:
    return f"{base_style}{_CHATROOM_STYLE_SUFFIX}"


def _shuffled_archetype_sequence(rng: random.Random, total_players: int) -> list[dict[str, object]]:
    sequence: list[dict[str, object]] = []
    while len(sequence) < total_players:
        pool = list(_ARCHETYPES)
        rng.shuffle(pool)
        sequence.extend(pool)
    return sequence[:total_players]


def generate_mafia_personas(room_id: str, total_players: int) -> list[AgentConfig]:
    rng = random.Random(f"mafia:{room_id}:{total_players}")
    names = list(_NAMES)
    rng.shuffle(names)
    archetypes = _shuffled_archetype_sequence(rng, total_players)
    personas: list[AgentConfig] = []
    for index in range(total_players):
        name = names[index % len(names)]
        archetype = dict(archetypes[index])
        personality = PersonalityConfig(**archetype["personality"])
        goals = list(archetype["goals"])
        goals.append(f"Feel like a real {archetype['label'].lower()} at the table.")
        agent_id = f"{name.lower()}-{index + 1}"
        personas.append(
            AgentConfig(
                id=agent_id,
                display_name=name,
                goals=goals,
                style_prompt=_chatroom_style(archetype["style_prompt"]),
                max_words=archetype["max_words"],
                personality=personality,
                scheduler=SchedulerConfig(tick_rate_seconds=0.6),
                generation=GenerationConfig(tick_rate_seconds=0.4, buffer_size=1, staleness_window_seconds=7.0),
            )
        )
    return personas


def reroll_persona(room_id: str, total_players: int, index: int, existing_names: Iterable[str] = ()) -> AgentConfig:
    rng = random.Random(f"mafia:{room_id}:{total_players}:reroll:{index}")
    taken = {name.strip() for name in existing_names if name.strip()}
    names = [name for name in _NAMES if name not in taken] or list(_NAMES)
    name = rng.choice(names)
    archetype = dict(rng.choice(_ARCHETYPES))
    return AgentConfig(
        id=f"{name.lower()}-{index + 1}",
        display_name=name,
        goals=[*archetype["goals"], f"Feel like a real {archetype['label'].lower()} at the table."],
        style_prompt=_chatroom_style(archetype["style_prompt"]),
        max_words=archetype["max_words"],
        personality=PersonalityConfig(**archetype["personality"]),
        scheduler=SchedulerConfig(tick_rate_seconds=0.6),
        generation=GenerationConfig(tick_rate_seconds=0.4, buffer_size=1, staleness_window_seconds=7.0),
    )
