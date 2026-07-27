"""Compare unresolved CNInfo special actions with persisted TDX XDXR rows."""

from __future__ import annotations

import argparse
import asyncio
import json
from pathlib import Path
from typing import Any, Dict

from scheduler.tasks import data_manager


async def _run(args: argparse.Namespace) -> Dict[str, Any]:
    return await data_manager.govern_cninfo_corporate_action_resolutions(
        start_date=args.start_date,
        end_date=args.end_date,
        exchanges=["SSE", "SZSE"],
        scopes=["inventory", "tdx_asymmetric_review"],
        max_events=args.max_events,
        target_offset=args.target_offset,
        dry_run=not args.write,
        download_documents=False,
        run_ocr=False,
        refresh_documents=False,
        classify_titles_with_llm=False,
        exclude_reviewed_events=True,
        sample_limit=args.sample_limit,
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="1990-12-19")
    parser.add_argument("--end-date", default="2026-07-24")
    parser.add_argument("--max-events", type=int, default=400)
    parser.add_argument("--target-offset", type=int, default=0)
    parser.add_argument("--sample-limit", type=int, default=100)
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--output")
    args = parser.parse_args()

    result = asyncio.run(_run(args))
    payload = json.dumps(result, ensure_ascii=False, indent=2, default=str)
    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(payload + "\n", encoding="utf-8")
    else:
        print(payload)


if __name__ == "__main__":
    main()
