"""Export accepted company facts and explicit gaps from an existing batch."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from research.company_profile.shadow_batch_service import (
    ManufacturingMaterialsShadowBatchService,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-directory", required=True, type=Path)
    parser.add_argument("--output-directory", required=True, type=Path)
    args = parser.parse_args()
    result = ManufacturingMaterialsShadowBatchService().export_accepted_research_data(
        batch_directory=args.batch_directory, output_directory=args.output_directory
    )
    print(
        json.dumps(
            {
                "accepted_fact_count": result["accepted_fact_count"],
                "reports": result["reports"],
                "output": str(args.output_directory / "accepted-research-data.json"),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
