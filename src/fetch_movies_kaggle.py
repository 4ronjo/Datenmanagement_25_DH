from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Iterable

DATASET = "rounakbanik/the-movies-dataset"  # Kaggle slug  :contentReference[oaicite:2]{index=2}

# Minimal sinnvoll für ETL + Dashboard + Graph:
DEFAULT_TABLES = [
    "movies_metadata.csv",
    "credits.csv",
    "keywords.csv",
    # ratings.csv ist groß; für Uni/MVP reicht oft ratings_small.csv
    "ratings_small.csv",
]

OPTIONAL_TABLES = [
    "ratings.csv",
    "links.csv",
    "links_small.csv",
]


def _ensure_kaggle_token(base: Path) -> Path | None:
    """Find Kaggle token (prefers repo-local data/raw) and configure env accordingly."""
    env_dir = os.environ.get("KAGGLE_CONFIG_DIR")
    candidates = []
    if env_dir:
        candidates.append(Path(env_dir))
    candidates += [
        base / "raw",
        Path.home() / ".kaggle",
    ]

    for candidate in candidates:
        config = candidate / "kaggle.json"
        if config.exists():
            os.environ.setdefault("KAGGLE_CONFIG_DIR", str(candidate))
            try:
                config.chmod(0o600)
            except PermissionError:
                # Best effort; Kaggle only warns about permissions.
                pass
            return candidate

    if os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY"):
        return None

    msg = (
        "Kaggle API Token nicht gefunden. Lege kaggle.json in data/raw/ oder ~/.kaggle/ ab "
        "oder setze KAGGLE_USERNAME/KAGGLE_KEY."
    )
    raise FileNotFoundError(msg)


def download_and_unzip(dataset: str, raw_dir: Path) -> None:
    """Download + unzip using official Kaggle API (python package)."""
    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
    except ImportError as e:
        raise ImportError(
            "Python-Paket 'kaggle' fehlt. Installiere es mit: pip install kaggle"
        ) from e

    raw_dir.mkdir(parents=True, exist_ok=True)

    api = KaggleApi()
    api.authenticate()

    # lädt ZIP und entpackt direkt
    api.dataset_download_files(dataset, path=str(raw_dir), unzip=True, quiet=False)


def select_tables(raw_dir: Path, selected_dir: Path, tables: Iterable[str]) -> None:
    selected_dir.mkdir(parents=True, exist_ok=True)
    missing = []

    for name in tables:
        src = raw_dir / name
        if not src.exists():
            missing.append(name)
            continue
        dst = selected_dir / name
        # Copy (so raw bleibt raw)
        dst.write_bytes(src.read_bytes())

    if missing:
        print(
            "\nWARNUNG: Folgende Dateien wurden im entpackten Dataset nicht gefunden:\n"
            + "\n".join(f" - {m}" for m in missing)
            + "\n\nTipp: Manche Varianten enthalten ratings.csv statt ratings_small.csv."
        )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Fetch & unpack Kaggle 'The Movies Dataset' into data/raw and copy selected tables."
    )
    parser.add_argument("--out", default="data", help="Basisordner (default: data)")
    parser.add_argument(
        "--include-full-ratings",
        action="store_true",
        help="Zusätzlich ratings.csv statt nur ratings_small.csv selektieren (groß!).",
    )
    args = parser.parse_args()

    base = Path(args.out)
    raw_dir = base / "raw" / "kaggle_movies"
    selected_dir = base / "raw_selected" / "kaggle_movies"

    try:
        _ensure_kaggle_token(base)
    except FileNotFoundError as e:
        print(str(e), file=sys.stderr)
        return 2

    print(f"[1/3] Download & unzip: {DATASET} -> {raw_dir}")
    download_and_unzip(DATASET, raw_dir)

    tables = list(DEFAULT_TABLES)
    if args.include_full_ratings:
        # ersetze small durch full (wenn gewünscht)
        if "ratings_small.csv" in tables:
            tables.remove("ratings_small.csv")
        tables.append("ratings.csv")

    # Du kannst optional links.* auch immer mitnehmen:
    # tables += ["links.csv", "links_small.csv"]
    print(f"[2/3] Select tables -> {selected_dir}")
    select_tables(raw_dir, selected_dir, tables)

    # Manifest
    manifest = selected_dir / "_manifest.txt"
    manifest.write_text(
        "Selected tables:\n" + "\n".join(tables) + "\n",
        encoding="utf-8",
    )
    print(f"[3/3] Done. Manifest: {manifest}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
