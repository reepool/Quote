#!/usr/bin/env python
"""CLI wrapper for financial data_available_date backfill."""

from __future__ import annotations

import sys
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from research.financial_available_date_backfill import main


if __name__ == "__main__":
    raise SystemExit(main())
