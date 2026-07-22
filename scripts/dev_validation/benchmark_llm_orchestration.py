#!/usr/bin/env python3
"""Run a bounded, offline concurrency benchmark for common LLM orchestration."""

from __future__ import annotations

import argparse
import asyncio
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

from utils.llm import CallableTransport, LlmClient, LlmConfig, LlmMessage, LlmRequest


logging.getLogger("LLM").disabled = True


def _config(concurrency: int) -> LlmConfig:
    return LlmConfig.from_mapping({
        "enabled": True,
        "provider_resources": {
            "benchmark": {
                "provider": "openai_compatible",
                "hard_max_concurrency": 60,
                "default_bulk_concurrency": 50,
                "reserved_concurrency": 10,
                "http_max_connections": 70,
                "http_max_keepalive_connections": 60,
            }
        },
        "profiles": {
            "benchmark": {
                "enabled": True,
                "provider_resource": "benchmark",
                "base_url": "https://benchmark.invalid/v1",
                "api_key_env": "BENCHMARK_LLM_KEY",
                "model": "offline-benchmark",
                "max_concurrency": concurrency,
                "requests_per_minute": 0,
                "max_retries": 0,
            }
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

    async def transport(url, headers, payload, timeout):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(delay_seconds)
        active -= 1
        return {
            "choices": [{
                "message": {"content": "x" * output_characters},
                "finish_reason": "stop",
            }],
            "id": f"response-{peak}-{time.monotonic_ns()}",
        }

    client = LlmClient(
        _config(concurrency),
        transport=CallableTransport(transport),
        environment={"BENCHMARK_LLM_KEY": "offline"},
        owns_transport=True,
    )
    payload = "p" * input_characters
    before_fds = len(os.listdir("/proc/self/fd"))
    tracemalloc.start()
    started = time.monotonic()
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
    finally:
        await client.close()
    elapsed = time.monotonic() - started
    _, traced_peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    after_fds = len(os.listdir("/proc/self/fd"))
    request_ids = {response.request_id for response in responses}
    request_hashes = {response.request_hash for response in responses}
    business_keys = {
        response.lineage.get("business_item_key") for response in responses
    }
    return {
        "concurrency": concurrency,
        "elapsed_seconds": round(elapsed, 6),
        "peak_transport_concurrency": peak,
        "python_peak_bytes": traced_peak,
        "max_rss_kib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        "fd_delta": after_fds - before_fds,
        "unique_request_ids": len(request_ids),
        "unique_request_hashes": len(request_hashes),
        "unique_business_keys": len(business_keys),
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
