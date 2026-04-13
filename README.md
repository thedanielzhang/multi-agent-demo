# mafia

`mafia` is an event-driven multi-agent conversation harness for experimenting with group chat behavior. It can run as a local CLI, a FastAPI web service, or a browser-based chatroom, and it also includes a dedicated "mafia" room mode that turns the chat into a social-deduction game.

This repository contains:

- A core conversation engine built around commands, events, projections, and worker loops
- Runtime adapters for `scripted`, `claude`, and `codex`
- A FastAPI service with a React-based browser UI and websocket chat updates
- A playable mafia game flow layered on top of the same chat infrastructure
- Test coverage for the engine, service, and runtime integrations

## What Is In This Repo

### Main Python package

`src/mafia/`

- `engine.py`: core orchestration for runs, event dispatch, clocks, transports, and worker startup
- `service.py`: FastAPI app, REST endpoints, websocket room handling, and multi-room management
- `cli.py`: simple command-line entrypoint that runs a config and prints the resulting event log
- `config.py`: Pydantic config models for runtimes, agents, chat behavior, room mode, and mafia settings
- `messages.py`: shared event, command, message, and mafia state models
- `projections.py`: read models derived from the append-only event log
- `workers.py`: scheduler, generator, delivery, topic analysis, and voting workers
- `policies.py`: prompt, scheduling, context, and generation policy decisions
- `agent.py`: runtime-facing agent invocation helpers
- `transport.py`: message transport abstraction with the built-in `loopback` transport
- `runtime_support.py`: in-memory session storage and per-run workspace creation
- `mafia_controller.py`: mafia game state machine and phase transitions
- `mafia_personas.py`: persona generation for mafia-mode agent players
- `scripted_logic.py`: deterministic scripted behavior used for local testing and smoke runs
- `web_pages.py`: HTML shell returned by the service
- `static/react-app.js` and `static/react-app.css`: front-end application assets
- `runtimes/`: provider-specific runtime adapters
  - `scripted.py`: no-external-dependency runtime for tests and local iteration
  - `claude.py`: Claude SDK-backed runtime
  - `codex.py`: Codex CLI-backed runtime

### Tests

`tests/`

- `test_engine.py`: engine, event ordering, mafia flow, timing, and worker behavior
- `test_service.py`: HTTP routes, websockets, room creation, and UI/API behavior
- `test_runtimes.py`: runtime normalization, validation, and provider adapter behavior
- `conftest.py`: shared fixtures and sample configs

### Project-level files

- `pyproject.toml`: package metadata, dependencies, and console script entrypoints
- `local-config.yaml`: example config for a Claude-backed room
- `local-config-scripted.yaml`: example config for a fully local scripted run
- `DIRECTORY_MAP.md`: currently a placeholder for higher-level repo/dependency mapping

### Generated or local-only artifacts

- `.mafia-workspaces/`: per-run working directories created by the engine at runtime
- `__pycache__/` and `.pytest_cache/`: standard local Python caches

## Core Concepts

- `room_mode`
  - `regular`: normal multi-agent group chat
  - `mafia`: lobby + day/night phases, hidden roles, private state, and vote handling
- `mode`
  - `baseline.time_to_talk`: closer to a turn-taking baseline
  - `improved.buffered_async`: buffered asynchronous generation with topic/context support
- `runtime`
  - selects how agent decisions are produced
- `transport`
  - selects how candidate messages are committed; this repo currently ships with `loopback`

Each run builds a `ConversationEngine`, appends events to an in-memory event log, updates projections, and lets workers schedule or generate new actions from those projections.

## Getting Started

### Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

### Runtime requirements

- `scripted`: works out of the box and is the easiest way to explore the project locally
- `claude`: requires the `claude-agent-sdk` package in addition to this repo's base dependencies
- `codex`: requires the `codex` CLI to be installed and available on `PATH`

## Running It

### CLI

Run a config and print the exported event log:

```bash
mafia local-config-scripted.yaml --message "Where should we go for lunch?"
```

### Web service

Start the FastAPI app:

```bash
mafia-service local-config-scripted.yaml --host 127.0.0.1 --port 8000
```

Then open [http://127.0.0.1:8000](http://127.0.0.1:8000).

The service exposes:

- `/` and `/config` for the default room UI
- `/rooms/{room_id}` and `/rooms/{room_id}/config` for named rooms
- `/api/config`, `/api/debug`, `/api/messages`, and `/status`
- `/start`, `/pause`, `/resume`, and `/stop`
- room-scoped API variants under `/api/rooms/{room_id}/...`
- `/ws` and `/ws/{room_id}` for websocket clients

## Configuration Shape

Configs are JSON or YAML and are loaded into `AppConfig`. The main top-level sections are:

- `room_mode`
- `mode`
- `runtime`
- `transport`
- `chat`
- `generation`
- `topic`
- `mafia`
- `context_defaults`
- `agents`

Each agent defines:

- `id`
- `display_name`
- `goals`
- `style_prompt`
- `max_words`
- `personality`
- `scheduler`
- `generation`
- optional per-agent `context`

The two example configs in the repo are the best starting point.

## Development

Run the test suite:

```bash
pytest
```

Lint with Ruff:

```bash
ruff check .
```

## Notes

- The current README describes the code that is present today; `DIRECTORY_MAP.md` is still mostly empty.
- The repository already includes a large runtime workspace tree under `.mafia-workspaces/`, but that directory is generated data rather than core source code.
