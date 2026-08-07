#!/usr/bin/env python3
"""Run a bounded, offline concurrency benchmark for common LLM orchestration."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
import logging
import os
from pathlib import Path
import resource
import sys
import time
import tracemalloc


PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.llm import (
    CallableTransport,
    LlmClient,
    LlmConfig,
    LlmMessage,
    LlmPoolCoordinatorRegistry,
    LlmRequest,
)
from utils.llm.orchestration import ProviderCoordinatorRegistry
from utils.llm.rate_limit import ProfileLimiterRegistry


logging.getLogger("LLM").disabled = True


def _config(concurrency: int) -> LlmConfig:
    return LlmConfig.from_mapping({
        "enabled": True,
        "provider_resources": {
            "benchmark-grok": {
                "provider": "openai_compatible",
                "hard_max_concurrency": 60,
                "default_bulk_concurrency": 50,
                "reserved_concurrency": 10,
                "http_max_connections": 70,
                "http_max_keepalive_connections": 60,
                "requests_per_minute": 0,
            },
            "benchmark-luna": {
                "provider": "openai_compatible",
                "hard_max_concurrency": 60,
                "default_bulk_concurrency": 50,
                "reserved_concurrency": 10,
                "http_max_connections": 70,
                "http_max_keepalive_connections": 60,
                "requests_per_minute": 0,
            }
        },
        "profiles": {
            "benchmark__grok": {
                "enabled": True,
                "provider": "openai_compatible",
                "provider_resource": "benchmark-grok",
                "source_label": "offline:grok",
                "base_url": "https://benchmark.invalid/v1",
                "endpoint": "/v1/chat/completions",
                "api_key_env": "BENCHMARK_LLM_KEY",
                "model": "offline-grok",
                "structured_output_mode": "auto",
                "supported_structured_output_modes": ["json_object"],
                "allow_prompt_only": True,
                "timeout_seconds": 30,
                "queue_timeout_seconds": 5,
                "attempt_timeout_seconds": 10,
                "max_concurrency": concurrency,
                "requests_per_minute": 0,
                "max_retries": 0,
                "max_schema_repair_attempts": 0,
                "temperature": 0.0,
                "max_output_tokens_field": "max_tokens",
                "stream": False,
                "stream_include_usage": True,
                "max_retry_after_seconds": 1,
                "retry_backoff_seconds": 0,
                "retry_jitter_ratio": 0,
                "idempotency_header": "Idempotency-Key",
            },
            "benchmark__luna": {
                "enabled": True,
                "provider": "openai_compatible",
                "provider_resource": "benchmark-luna",
                "source_label": "offline:luna",
                "base_url": "https://benchmark.invalid/v1",
                "endpoint": "/v1/chat/completions",
                "api_key_env": "BENCHMARK_LLM_KEY",
                "model": "offline-luna",
                "structured_output_mode": "auto",
                "supported_structured_output_modes": ["json_object"],
                "allow_prompt_only": True,
                "timeout_seconds": 30,
                "queue_timeout_seconds": 5,
                "attempt_timeout_seconds": 10,
                "max_concurrency": concurrency,
                "requests_per_minute": 0,
                "max_retries": 0,
                "max_schema_repair_attempts": 0,
                "temperature": 0.0,
                "max_output_tokens_field": "max_tokens",
                "stream": False,
                "stream_include_usage": True,
                "max_retry_after_seconds": 1,
                "retry_backoff_seconds": 0,
                "retry_jitter_ratio": 0,
                "idempotency_header": "Idempotency-Key",
            },
        },
        "pools": {
            "benchmark-pool": {
                "enabled": True,
                "total_concurrency": concurrency,
                "queue_size": max(1, concurrency * 2),
                "strategy": "weighted_fair",
                "borrow_idle_capacity": True,
                "members": [
                    {
                        "source_label": "offline:grok",
                        "weight": 3,
                        "profiles": {"benchmark": "benchmark__grok"},
                    },
                    {
                        "source_label": "offline:luna",
                        "weight": 1,
                        "profiles": {"benchmark": "benchmark__luna"},
                    },
                ],
                "failover": {"enabled": True, "max_hops": 1},
            },
        },
        "routes": {
            "benchmark": {
                "pool": "benchmark-pool",
                "revision": "offline-benchmark-v1",
            },
        },
    })


async def _run_level(
    concurrency: int,
    *,
    input_characters: int,
    output_characters: int,
    delay_seconds: float,
) -> dict[str, object]:
    active = 0
    peak = 0
    first_event_latencies: list[float] = []
    transport_latencies: list[float] = []

    async def transport(url, headers, payload, timeout):
        nonlocal active, peak
        started = time.monotonic()
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(delay_seconds)
        first_event_latencies.append((time.monotonic() - started) * 1000)
        await asyncio.sleep(0)
        active -= 1
        transport_latencies.append((time.monotonic() - started) * 1000)
        return {
            "model": payload.get("model"),
            "choices": [{
                "message": {"content": "x" * output_characters},
                "finish_reason": "stop",
            }],
            "id": f"response-{peak}-{time.monotonic_ns()}",
        }

    config = _config(concurrency)
    pool_registry = LlmPoolCoordinatorRegistry()
    provider_registry = ProviderCoordinatorRegistry()
    limiter_registry = ProfileLimiterRegistry()
    client = LlmClient(
        config,
        transport=CallableTransport(transport),
        environment={"BENCHMARK_LLM_KEY": "offline"},
        owns_transport=True,
        limiter_registry=limiter_registry,
        provider_coordinator_registry=provider_registry,
        pool_coordinator_registry=pool_registry,
    )
    payload = "p" * input_characters
    before_fds = len(os.listdir("/proc/self/fd"))
    tracemalloc.start()
    started = time.monotonic()
    pool_snapshot = None
    shutdown_duration_ms = 0.0
    try:
        responses = await asyncio.gather(*(
            client.complete(LlmRequest(
                profile="benchmark",
                messages=[LlmMessage(
                    role="user",
                    content=f"{index}:{payload}",
                )],
                metadata={
                    "workload": "offline_benchmark",
                    "business_item_key": str(index),
                    "input_hash": f"benchmark-{index}",
                    "bulk": True,
                },
            ))
            for index in range(concurrency)
        ))
        pool_snapshot = pool_registry.get(config, "benchmark-pool").snapshot()
    finally:
        shutdown_started = time.monotonic()
        await client.close()
        await pool_registry.close_all()
        provider_registry.clear()
        limiter_registry.clear()
        shutdown_duration_ms = (time.monotonic() - shutdown_started) * 1000
    elapsed = time.monotonic() - started
    _, traced_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    after_fds = len(os.listdir("/proc/self/fd"))
    request_ids = {response.request_id for response in responses}
    request_hashes = {response.request_hash for response in responses}
    business_keys = {
        response.lineage.get("business_item_key") for response in responses
    }
    source_counts = {
        source: sum(response.source_label == source for response in responses)
        for source in ("offline:grok", "offline:luna")
    }
    assert pool_snapshot is not None
    return {
        "concurrency": concurrency,
        "elapsed_seconds": round(elapsed, 6),
        "peak_transport_concurrency": peak,
        "connection_count_peak": peak,
        "first_event_latency_ms": round(max(first_event_latencies or [0.0]), 3),
        "transport_latency_ms": round(max(transport_latencies or [0.0]), 3),
        "shutdown_duration_ms": round(shutdown_duration_ms, 3),
        "python_peak_bytes": traced_peak,
        "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "fd_delta": after_fds - before_fds,
        "unique_request_ids": len(request_ids),
        "unique_request_hashes": len(request_hashes),
        "unique_business_keys": len(business_keys),
        "source_counts": source_counts,
        "route_fingerprint": responses[0].route_fingerprint if responses else None,
        "pool_snapshot": asdict(pool_snapshot),
        "registry_empty_after_shutdown": pool_registry.snapshots() == {},
        "identity_ok": (
            len(request_ids)
            == len(request_hashes)
            == len(business_keys)
            == concurrency
        ),
    }


async def _main(args: argparse.Namespace) -> None:
    results = []
    for concurrency in args.concurrency:
        results.append(await _run_level(
            concurrency,
            input_characters=args.input_characters,
            output_characters=args.output_characters,
            delay_seconds=args.delay_seconds,
        ))
    print(json.dumps({
        "input_characters": args.input_characters,
        "output_characters": args.output_characters,
        "delay_seconds": args.delay_seconds,
        "results": results,
    }, ensure_ascii=False, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--concurrency", type=int, nargs="+", default=[10, 25, 50]
    )
    parser.add_argument("--input-characters", type=int, default=225_000)
    parser.add_argument("--output-characters", type=int, default=100_000)
    parser.add_argument("--delay-seconds", type=float, default=0.05)
    return parser.parse_args()


if __name__ == "__main__":
    asyncio.run(_main(parse_args()))
