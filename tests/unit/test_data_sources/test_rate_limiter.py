import time

import pytest

from data_sources.base_source import RateLimitConfig, RateLimiter


@pytest.mark.asyncio
async def test_rate_limiter_enforces_min_interval_seconds():
    limiter = RateLimiter(
        RateLimitConfig(
            max_requests_per_minute=1000,
            max_requests_per_hour=1000,
            max_requests_per_day=1000,
            min_interval_seconds=0.02,
        )
    )

    started = time.monotonic()
    await limiter.acquire()
    await limiter.acquire()
    elapsed = time.monotonic() - started

    assert elapsed >= 0.018
