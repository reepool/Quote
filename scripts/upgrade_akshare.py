#!/usr/bin/env python
"""akshare 受控升级入口。实际流程在 scripts/upgrade_market_dependency.py。

    python scripts/upgrade_akshare.py --target latest
    python scripts/upgrade_akshare.py --target 1.21.0
    python scripts/upgrade_akshare.py --dry-run
"""

from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def _load_upgrade_main():
    path = PROJECT_ROOT / "scripts" / "upgrade_market_dependency.py"
    spec = importlib.util.spec_from_file_location("upgrade_market_dependency", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.main


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--target", default="latest", help="目标版本号或 latest")
    parser.add_argument("--service", default="quote-system.service")
    parser.add_argument("--skip-restart", action="store_true")
    parser.add_argument("--notify-skip", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)
    package = "akshare" if args.target == "latest" else f"akshare=={args.target.strip()}"
    forwarded = ["--package", package, "--service", args.service]
    if args.skip_restart:
        forwarded.append("--skip-restart")
    if args.notify_skip:
        forwarded.append("--notify-skip")
    if args.dry_run:
        forwarded.append("--dry-run")
    return _load_upgrade_main()(forwarded)


if __name__ == "__main__":
    sys.exit(main())
