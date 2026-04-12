from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import Awaitable, Callable
from typing import Any

from mafia.messages import CommandEnvelope


CommandHandler = Callable[[CommandEnvelope], Awaitable[None]]
FailureHandler = Callable[[str, Exception], Awaitable[None]]


class CommandRouter:
    """Single-owner command router with completion-aware dispatch."""

    def __init__(self) -> None:
        self._handlers: dict[str, CommandHandler] = {}
        self._queues: dict[str, asyncio.Queue[CommandEnvelope]] = {}
        self._workers: dict[str, asyncio.Task[None]] = {}
        self._processed: set[str] = set()
        self._processed_order: deque[str] = deque()
        self._inflight: dict[str, asyncio.Future[None]] = {}
        self._closed = False
        self._failure_handler: FailureHandler | None = None
        self._processed_limit = 4096

    def register(self, subject: str, handler: CommandHandler) -> None:
        if subject in self._handlers:
            raise ValueError(f"handler already registered for {subject}")
        self._handlers[subject] = handler
        self._queues[subject] = asyncio.Queue(maxsize=64)

    async def start(self, failure_handler: FailureHandler | None = None) -> None:
        self._failure_handler = failure_handler
        for subject, handler in self._handlers.items():
            if subject in self._workers:
                continue
            task = asyncio.create_task(self._consume(subject, handler))
            self._workers[subject] = task

    async def close(self) -> None:
        self._closed = True
        for task in self._workers.values():
            task.cancel()
        await asyncio.gather(*self._workers.values(), return_exceptions=True)

    @property
    def worker_tasks(self) -> list[asyncio.Task[None]]:
        return list(self._workers.values())

    async def dispatch(self, command: CommandEnvelope) -> None:
        future = await self._submit(command)
        await asyncio.shield(future)

    async def enqueue(self, command: CommandEnvelope) -> None:
        future = await self._submit(command)
        future.add_done_callback(_drain_future_exception)

    async def _submit(self, command: CommandEnvelope) -> asyncio.Future[None]:
        if self._closed:
            raise RuntimeError("command router is closed")
        handler = self._handlers.get(command.subject)
        if handler is None:
            raise ValueError(f"no handler registered for {command.subject}")
        if command.command_id in self._processed:
            loop = asyncio.get_running_loop()
            done = loop.create_future()
            done.set_result(None)
            return done
        existing = self._inflight.get(command.command_id)
        if existing is not None:
            return existing
        loop = asyncio.get_running_loop()
        future: asyncio.Future[None] = loop.create_future()
        self._inflight[command.command_id] = future
        await self._queues[command.subject].put(command)
        return future

    async def _consume(self, subject: str, handler: CommandHandler) -> None:
        queue = self._queues[subject]
        while True:
            command = await queue.get()
            future = self._inflight.get(command.command_id)
            try:
                if command.command_id in self._processed:
                    if future and not future.done():
                        future.set_result(None)
                    continue
                await handler(command)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if future and not future.done():
                    future.set_exception(exc)
                self._inflight.pop(command.command_id, None)
                if self._failure_handler is not None:
                    await self._failure_handler(subject, exc)
            else:
                self._remember_processed(command.command_id)
                self._inflight.pop(command.command_id, None)
                if future and not future.done():
                    future.set_result(None)
            finally:
                queue.task_done()

    def _remember_processed(self, command_id: str) -> None:
        self._processed.add(command_id)
        self._processed_order.append(command_id)
        while len(self._processed_order) > self._processed_limit:
            removed = self._processed_order.popleft()
            self._processed.discard(removed)


def _drain_future_exception(future: asyncio.Future[None]) -> None:
    if future.cancelled():
        return
    try:
        future.exception()
    except Exception:
        return
