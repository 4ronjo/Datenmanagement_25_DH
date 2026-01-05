"""
Run the full movie ETL pipeline.
"""

from __future__ import annotations

import argparse
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

# Allow running as a script from the src/ folder by injecting the repo root.
ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src import (
    fetch_movies_kaggle,
    step01_profile_raw,
    step02_transform_processed,
    step03_build_curated,
    step04_export_neo4j,
)
from src.config import ensure_directories, inputs, paths


def _log(msg: str, log_path: Path) -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    line = f"[{timestamp} UTC] {msg}"
    print(line)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def run_pipeline(
    skip_profile: bool, skip_neo4j: bool, output_format: str, skip_extract: bool = False
) -> None:
    ensure_directories()
    log_path = paths.docs_dir / "pipeline_run.log"

    def raw_ready() -> bool:
        expected = [
            paths.raw_selected_dir / inputs.movies_metadata,
            paths.raw_selected_dir / inputs.credits,
            paths.raw_selected_dir / inputs.keywords,
            paths.raw_selected_dir / inputs.ratings_small,
            paths.raw_selected_dir / inputs.links_small,
        ]
        return all(p.exists() for p in expected)

    steps = [
        (
            "fetch_raw",
            lambda: fetch_movies_kaggle.download_movies(
                target_dir=paths.raw_selected_dir
            ),
            (not skip_extract) and (not raw_ready()),
        ),
        ("profile_raw", step01_profile_raw.main, not skip_profile),
        ("transform_processed", lambda: step02_transform_processed.main(output_format), True),
        ("build_curated", lambda: step03_build_curated.main(output_format), True),
        ("export_neo4j", lambda: step04_export_neo4j.main(output_format), not skip_neo4j),
    ]

    for name, func, should_run in steps:
        if not should_run:
            _log(f"Skipping step {name}", log_path)
            continue
        _log(f"Starting step {name}", log_path)
        try:
            func()
            _log(f"Completed step {name}", log_path)
        except Exception as exc:  # pragma: no cover - runtime safeguard
            _log(f"FAILED step {name}: {exc}", log_path)
            _log(traceback.format_exc(), log_path)
            raise SystemExit(f"Pipeline aborted at {name}: {exc}") from exc


def main() -> None:
    parser = argparse.ArgumentParser(description="Run movie ETL pipeline.")
    parser.add_argument(
        "--skip-profile",
        action="store_true",
        help="Skip raw profiling step.",
    )
    parser.add_argument(
        "--skip-extract",
        action="store_true",
        help="Skip Kaggle extract (only if raw_selected is already populated).",
    )
    parser.add_argument(
        "--skip-neo4j",
        action="store_true",
        help="Skip Neo4j export step.",
    )
    parser.add_argument(
        "--format",
        dest="output_format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Output format for processed/curated tables.",
    )
    args = parser.parse_args()
    run_pipeline(
        skip_profile=args.skip_profile,
        skip_neo4j=args.skip_neo4j,
        output_format=args.output_format,
        skip_extract=args.skip_extract,
    )


if __name__ == "__main__":
    main()
