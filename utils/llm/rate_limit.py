"""Profile-shared concurrency and requests-per-minute limits."""

from __future__ import annotations

import asyncio
import time
from collections import deque
from contextlib import asynccontextmanager
from typing import AsyncIterator, Callable

from .errors import LlmDeadlineExceededError


class ProfileLimiter:
    def __init__(
        self,
        *,
        max_concurrency: int,
        requests_per_minute: int,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], object] = asyncio.sleep,
        window_seconds: float = 60.0,
    ) -> None:
        self._semaphore = asyncio.Semaphore(max(1, max_concurrency))
        self._rpm = max(0, requests_per_minute)
        self._clock = clock
        self._sleeper = sleeper
        self._window_seconds = max(0.01, window_seconds)
        self._timestamps: deque[float] = deque()
        self._rate_lock = asyncio.Lock()

    async def _wait_with_deadline(self, awaitable: object, deadline: float) -> object:
        remaining = deadline - self._clock()
        if remaining <= 0:
            raise LlmDeadlineExceededError()
        return await asyncio.wait_for(awaitable, timeout=remaining)  # type: ignore[arg-type]

    async def acquire(self, deadline: float) -> None:
        try:
            await self._wait_with_deadline(self._semaphore.acquire(), deadline)
            acquired = True
            if self._rpm:
                while True:
                    async with self._rate_lock:
                        now = self._clock()
                        while (
                            self._timestamps and now - self._timestamps[0] >= self._window_seconds
                        ):
                            self._timestamps.popleft()
                        if len(self._timestamps) < self._rpm:
                            self._timestamps.append(now)
                            break
                        wait_for = self._timestamps[0] + self._window_seconds - now
                    await self._wait_with_deadline(self._sleeper(wait_for), deadline)
        except asyncio.TimeoutError as exc:
            if "acquired" in locals() and acquired:
                self._semaphore.release()
            raise LlmDeadlineExceededError() from exc
        except BaseException:
            if "acquired" in locals() and acquired:
                self._semaphore.release()
            raise

    def release(self) -> None:
        self._semaphore.release()

    @asynccontextmanager
    async def slot(self, deadline: float) -> AsyncIterator[None]:
        await self.acquire(deadline)
        try:
            yield
        finally:
            self.release()


class ProfileLimiterRegistry:
    def __init__(self) -> None:
        self._limiters: dict[tuple[str, int, int], ProfileLimiter] = {}
        self._loop_id: int | None = None

    def get(
        self, profile_name: str, *, max_concurrency: int, requests_per_minute: int
    ) -> ProfileLimiter:
        loop_id = id(asyncio.get_running_loop())
        if self._loop_id != loop_id:
            self._limiters.clear()
            self._loop_id = loop_id
        key = (profile_name, max_concurrency, requests_per_minute)
        limiter = self._limiters.get(key)
        if limiter is None:
            limiter = ProfileLimiter(
                max_concurrency=max_concurrency,
                requests_per_minute=requests_per_minute,
            )
            self._limiters[key] = limiter
        return limiter

    def clear(self) -> None:
        self._limiters.clear()
        self._loop_id = None
