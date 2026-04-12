from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from fnmatch import fnmatchcase
from typing import Any


MessageHandler = Callable[[str, Any], Awaitable[None]]


@dataclass
class _Subscription:
    pattern: str
    maxsize: int
    overflow: str
    handler: MessageHandler
    queue: asyncio.Queue[tuple[str, Any]] = field(init=False)
    task: asyncio.Task[None] | None = None

    def __post_init__(self) -> None:
        self.queue = asyncio.Queue(maxsize=self.maxsize)


class SubjectBus:
    """In-process subject bus with bounded subscriber queues."""

    def __init__(self) -> None:
        self._subscriptions: list[_Subscription] = []
        self._closed = False

    async def close(self) -> None:
        self._closed = True
        for subscription in self._subscriptions:
            if subscription.task:
                subscription.task.cancel()
        await asyncio.gather(
            *[sub.task for sub in self._subscriptions if sub.task],
            return_exceptions=True,
        )

    def subscribe(
        self,
        pattern: str,
        handler: MessageHandler,
        *,
        maxsize: int = 64,
        overflow: str = "block",
    ) -> None:
        subscription = _Subscription(
            pattern=pattern,
            maxsize=maxsize,
            overflow=overflow,
            handler=handler,
        )
        subscription.task = asyncio.create_task(self._consume(subscription))
        self._subscriptions.append(subscription)

    async def publish(self, subject: str, message: Any) -> None:
        if self._closed:
            return
        for subscription in self._subscriptions:
            if not fnmatchcase(subject, subscription.pattern):
                continue
            await self._enqueue(subscription, subject, message)

    async def _enqueue(self, subscription: _Subscription, subject: str, message: Any) -> None:
        if subscription.overflow == "drop_oldest" and subscription.queue.full():
            with contextlib.suppress(asyncio.QueueEmpty):
                subscription.queue.get_nowait()
                subscription.queue.task_done()
        if subscription.overflow == "drop_newest" and subscription.queue.full():
            return
        await subscription.queue.put((subject, message))

    async def _consume(self, subscription: _Subscription) -> None:
        while True:
            subject, message = await subscription.queue.get()
            try:
                await subscription.handler(subject, message)
            finally:
                subscription.queue.task_done()
