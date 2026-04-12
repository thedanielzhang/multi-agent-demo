from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from mafia.engine import ConversationEngine, load_config


async def _run(args: argparse.Namespace) -> None:
    config = load_config(Path(args.config))
    engine = ConversationEngine(config)
    await engine.start()
    if args.message:
        await engine.submit_message(text=args.message)
    await engine.run()
    events = await engine.export_events()
    for logged in events:
        print(f"{logged.seq:04d} {logged.event.subject} {logged.event.payload}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mafia")
    parser.add_argument("config", help="Path to a JSON or YAML config file")
    parser.add_argument("--message", help="Optional initial human message", default=None)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    asyncio.run(_run(args))


if __name__ == "__main__":
    main()
