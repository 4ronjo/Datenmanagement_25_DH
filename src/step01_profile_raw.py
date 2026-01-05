"""
Profile raw Kaggle movie files and write a data quality report.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd

from src.config import ensure_directories, inputs, paths


def _load_csv_with_fallback(path: Path) -> pd.DataFrame:
    """Load CSV trying UTF-8 first, falling back to latin-1."""
    errors: list[Exception] = []
    for enc in ("utf-8", "latin-1"):
        try:
            return pd.read_csv(path, encoding=enc, low_memory=False)
        except Exception as exc:  # pragma: no cover - runtime safeguard
            errors.append(exc)
            continue
    raise RuntimeError(f"Failed to read {path} with UTF-8 and latin-1: {errors}")


def _profile_dataframe(df: pd.DataFrame) -> Dict[str, Any]:
    total_rows = len(df)
    missing_counts = df.isna().sum()
    missing_pct = (missing_counts / total_rows * 100).round(2) if total_rows else 0
    return {
        "rows": int(total_rows),
        "columns": int(df.shape[1]),
        "column_names": df.columns.tolist(),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "missing": {
            col: {"count": int(missing_counts[col]), "pct": float(missing_pct[col])}
            for col in df.columns
        },
        "duplicate_rows": int(df.duplicated().sum()),
        "memory_bytes": int(df.memory_usage(deep=True).sum()),
    }


def _safe_numeric(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.astype("Int64")


def _join_checks(
    movies_df: pd.DataFrame, credits_df: pd.DataFrame, keywords_df: pd.DataFrame
) -> Dict[str, Any]:
    movies_ids = set(_safe_numeric(movies_df.get("id", pd.Series(dtype="Int64"))).dropna())
    credits_ids = set(
        _safe_numeric(credits_df.get("id", pd.Series(dtype="Int64"))).dropna()
    )
    keywords_ids = set(
        _safe_numeric(keywords_df.get("id", pd.Series(dtype="Int64"))).dropna()
    )

    def compare(left: set[int], right: set[int]) -> Dict[str, int]:
        return {
            "matches": len(left & right),
            "missing_in_right": len(left - right),
            "missing_in_left": len(right - left),
        }

    return {
        "movies_vs_credits": compare(movies_ids, credits_ids),
        "movies_vs_keywords": compare(movies_ids, keywords_ids),
    }


def _ratings_key_check(
    ratings_df: pd.DataFrame, movies_df: pd.DataFrame, links_df: pd.DataFrame
) -> Dict[str, Any]:
    movie_ids_tmdb = set(_safe_numeric(movies_df.get("id", pd.Series(dtype="Int64"))).dropna())
    rating_ids_ml = set(_safe_numeric(ratings_df.get("movieId", pd.Series(dtype="Int64"))).dropna())

    links_df = links_df.rename(columns={"movieId": "movieId", "tmdbId": "tmdbId"}).copy()
    links_df["movieId"] = _safe_numeric(links_df.get("movieId", pd.Series(dtype="Int64")))
    links_df["tmdbId"] = _safe_numeric(links_df.get("tmdbId", pd.Series(dtype="Int64")))
    links_df = links_df.dropna(subset=["movieId", "tmdbId"])
    mapped_tmdb_ids = set(links_df["tmdbId"])

    mapped_matches = len(movie_ids_tmdb & mapped_tmdb_ids)

    return {
        "key_column_ratings": "movieId (MovieLens)",
        "key_column_movies": "id (TMDB)",
        "direct_overlap": len(movie_ids_tmdb & rating_ids_ml),
        "requires_mapping_via_links_small": True,
        "mapped_matches": mapped_matches,
        "movies_without_ratings_after_mapping": len(movie_ids_tmdb - mapped_tmdb_ids),
        "ratings_without_movies_after_mapping": len(mapped_tmdb_ids - movie_ids_tmdb),
    }


def _markdown_report(report: Dict[str, Any]) -> str:
    lines: List[str] = ["# Raw Data Profile", ""]
    for name, details in report["files"].items():
        lines.append(f"## {name}")
        lines.append(f"- Rows: {details['rows']}")
        lines.append(f"- Columns: {details['columns']}")
        lines.append(f"- Column names: {', '.join(details['column_names'])}")
        lines.append(f"- Dtypes: {', '.join(f'{k}={v}' for k, v in details['dtypes'].items())}")
        lines.append("- Missing values:")
        for col, missing in details["missing"].items():
            lines.append(
                f"  - {col}: {missing['count']} ({missing['pct']}%)"
            )
        lines.append(f"- Duplicate rows: {details['duplicate_rows']}")
        lines.append(f"- Memory usage (bytes): {details['memory_bytes']}")
        lines.append("")

    lines.append("## Join Checks")
    join = report["join_checks"]
    lines.append(
        f"- Movies vs Credits: matches={join['movies_vs_credits']['matches']}, "
        f"missing_in_credits={join['movies_vs_credits']['missing_in_right']}, "
        f"missing_in_movies={join['movies_vs_credits']['missing_in_left']}"
    )
    lines.append(
        f"- Movies vs Keywords: matches={join['movies_vs_keywords']['matches']}, "
        f"missing_in_keywords={join['movies_vs_keywords']['missing_in_right']}, "
        f"missing_in_movies={join['movies_vs_keywords']['missing_in_left']}"
    )
    ratings = report["ratings_key_check"]
    lines.append(
        f"- Ratings vs Movies (different ID spaces): ratings={ratings['key_column_ratings']}, "
        f"movies={ratings['key_column_movies']}"
    )
    lines.append(
        f"- Direct overlap (should be low): {ratings['direct_overlap']}"
    )
    lines.append(
        f"- Mapping via links_small required: {ratings['requires_mapping_via_links_small']}"
    )
    lines.append(
        f"- Mapped matches (movieId -> tmdbId): {ratings['mapped_matches']}"
    )
    lines.append(
        f"- Movies without ratings after mapping: {ratings['movies_without_ratings_after_mapping']}"
    )
    lines.append(
        f"- Ratings without movies after mapping: {ratings['ratings_without_movies_after_mapping']}"
    )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    ensure_directories()
    raw_dir = paths.raw_selected_dir
    docs_dir = paths.docs_dir

    files = {
        "movies_metadata": raw_dir / inputs.movies_metadata,
        "credits": raw_dir / inputs.credits,
        "keywords": raw_dir / inputs.keywords,
        "ratings_small": raw_dir / inputs.ratings_small,
        "links_small": raw_dir / inputs.links_small,
    }

    print("Profiling raw files from", raw_dir)
    dfs: Dict[str, pd.DataFrame] = {}
    for name, path in files.items():
        print(f"- Loading {name} ({path})")
        dfs[name] = _load_csv_with_fallback(path)

    report: Dict[str, Any] = {
        "files": {name: _profile_dataframe(df) for name, df in dfs.items()},
        "join_checks": _join_checks(dfs["movies_metadata"], dfs["credits"], dfs["keywords"]),
        "ratings_key_check": _ratings_key_check(
            dfs["ratings_small"], dfs["movies_metadata"], dfs["links_small"]
        ),
    }

    markdown = _markdown_report(report)

    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "raw_profile.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    (docs_dir / "raw_profile.md").write_text(markdown, encoding="utf-8")
    print(f"Reports written to {docs_dir}")


if __name__ == "__main__":
    main()
