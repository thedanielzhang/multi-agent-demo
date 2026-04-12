from __future__ import annotations

import asyncio

from mafia.messages import EventEnvelope, LoggedEvent


class EventLog:
    """Append-only in-memory event log with monotonic sequence numbers."""

    def __init__(self) -> None:
        self._events: list[LoggedEvent] = []
        self._condition = asyncio.Condition()

    @property
    def latest_seq(self) -> int:
        return self._events[-1].seq if self._events else 0

    async def append(self, event: EventEnvelope) -> LoggedEvent:
        async with self._condition:
            logged = LoggedEvent(seq=self.latest_seq + 1, event=event)
            self._events.append(logged)
            self._condition.notify_all()
            return logged

    async def snapshot(self, after_seq: int = 0) -> list[LoggedEvent]:
        return [event for event in self._events if event.seq > after_seq]

    async def wait_for_events(self, after_seq: int, timeout: float | None = None) -> list[LoggedEvent]:
        async with self._condition:
            if self.latest_seq <= after_seq:
                if timeout is None:
                    await self._condition.wait()
                else:
                    await asyncio.wait_for(self._condition.wait(), timeout=timeout)
            return [event for event in self._events if event.seq > after_seq]
