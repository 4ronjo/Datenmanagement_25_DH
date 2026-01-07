"""
Build deterministic dashboard insights from curated tables.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import pandas as pd

from src.config import ensure_directories, paths


def _find_table(name: str, curated_dir: Path) -> Tuple[Optional[Path], Optional[str]]:
    parquet_path = curated_dir / f"{name}.parquet"
    csv_path = curated_dir / f"{name}.csv"
    if parquet_path.exists():
        return parquet_path, "parquet"
    if csv_path.exists():
        return csv_path, "csv"
    return None, None


def _read_table(name: str, curated_dir: Path) -> Optional[pd.DataFrame]:
    path, fmt = _find_table(name, curated_dir)
    if not path:
        return None
    if fmt == "parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def _missing_pct_top10(df: pd.DataFrame) -> Dict[str, float]:
    if df.empty:
        return {}
    missing_pct = (df.isna().mean() * 100.0).sort_values(ascending=False).head(10)
    return {col: round(val, 2) for col, val in missing_pct.items()}


def _data_quality_block(tables: Dict[str, pd.DataFrame]) -> Dict[str, Any]:
    quality: Dict[str, Any] = {"tables": {}}
    for name, df in tables.items():
        if df is None:
            continue
        quality["tables"][name] = {
            "rows": int(df.shape[0]),
            "cols": int(df.shape[1]),
            "missing_pct": _missing_pct_top10(df),
            "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        }
    return quality


def _build_overview(movie_df: Optional[pd.DataFrame]) -> Dict[str, Any]:
    kpis: Dict[str, Any] = {}
    subtitle_parts = ["Data source: Kaggle The Movies Dataset."]
    if movie_df is not None and not movie_df.empty:
        movies_total = int(len(movie_df))
        kpis["movies_total"] = movies_total
        subtitle_parts.append(f"Curated movies: {movies_total}.")

        if "release_year" in movie_df.columns:
            years = _safe_numeric(movie_df["release_year"]).dropna().astype(int)
            if not years.empty:
                year_min = int(years.min())
                year_max = int(years.max())
                kpis["year_min"] = year_min
                kpis["year_max"] = year_max
                subtitle_parts.append(f"Period: {year_min}-{year_max}.")

        if "avg_rating" in movie_df.columns:
            avg_rating = _safe_numeric(movie_df["avg_rating"]).mean()
            if pd.notna(avg_rating):
                kpis["avg_rating_overall"] = round(float(avg_rating), 3)

    return {
        "title": "Movie & Collaboration Insights",
        "subtitle": " ".join(subtitle_parts),
        "kpis": kpis,
    }


def _build_trends(year_df: Optional[pd.DataFrame]) -> Dict[str, Any]:
    kpis: Dict[str, Any] = {}
    if year_df is not None and not year_df.empty:
        if "release_year" in year_df.columns:
            years = _safe_numeric(year_df["release_year"]).dropna().astype(int)
            kpis["years_count"] = int(years.nunique())

        count_col = "movie_count" if "movie_count" in year_df.columns else None
        if not count_col and "movies" in year_df.columns:
            count_col = "movies"
        if count_col and "release_year" in year_df.columns:
            temp = year_df.copy()
            temp["release_year"] = _safe_numeric(temp["release_year"])
            temp[count_col] = _safe_numeric(temp[count_col])
            temp = temp.dropna(subset=["release_year", count_col])
            if not temp.empty:
                top_row = temp.sort_values(count_col, ascending=False).iloc[0]
                kpis["top_year_by_movies"] = int(top_row["release_year"])

    return {
        "intro": "Time series of movies and key metrics by release year.",
        "kpis": kpis,
    }


def _build_roi(movie_df: Optional[pd.DataFrame], genre_df: Optional[pd.DataFrame]) -> Dict[str, Any]:
    kpis: Dict[str, Any] = {}
    notes = []

    if genre_df is not None and not genre_df.empty:
        kpis["genres_count"] = int(len(genre_df))
        if "avg_roi" in genre_df.columns and "genre_name" in genre_df.columns:
            temp = genre_df.copy()
            temp["avg_roi"] = _safe_numeric(temp["avg_roi"])
            temp = temp.dropna(subset=["avg_roi"])
            if not temp.empty:
                top = temp.sort_values("avg_roi", ascending=False).iloc[0]
                kpis["top_genre_by_roi"] = str(top["genre_name"])

    if movie_df is not None and not movie_df.empty and "budget" in movie_df.columns:
        budget = _safe_numeric(movie_df["budget"])
        total = len(budget)
        if total > 0:
            missing_or_zero = (budget.isna() | (budget <= 0)).mean()
            if missing_or_zero > 0.05:
                notes.append("Note: many budgets are missing/<=0, ROI can be distorted.")
        valid_budget = budget.dropna()
        if not valid_budget.empty:
            small_budget_share = (valid_budget < 100000).mean()
            if small_budget_share > 0.05:
                notes.append("Note: very small budgets (<100k) can create extreme ROI outliers.")

    return {
        "intro": "ROI & Success: compare budget, revenue, and ROI across genres.",
        "data_quality_notes": notes,
        "kpis": kpis,
    }


def _build_collab(graph_df: Optional[pd.DataFrame]) -> Dict[str, Any]:
    if graph_df is None or graph_df.empty:
        return {
            "intro": (
                "Graph insights file not found. Export the Neo4j query and store it as "
                "graph_insights_top_coactors.csv in data/curated."
            ),
            "kpis": {"coactor_pairs": 0},
            "top_pairs_preview": [],
        }

    required = {"actor_1", "actor_2", "shared_movies_count"}
    if not required.issubset(set(graph_df.columns)):
        return {
            "intro": "Graph insights file is missing required columns.",
            "kpis": {"coactor_pairs": 0},
            "top_pairs_preview": [],
        }

    temp = graph_df.copy()
    temp["shared_movies_count"] = _safe_numeric(temp["shared_movies_count"])
    temp = temp.dropna(subset=["shared_movies_count"])
    temp = temp.sort_values("shared_movies_count", ascending=False)

    coactor_pairs = int(len(temp))
    top_pair = {}
    if not temp.empty:
        top_pair = temp.iloc[0][["actor_1", "actor_2", "shared_movies_count"]].to_dict()

    preview = (
        temp.head(10)[["actor_1", "actor_2", "shared_movies_count"]]
        .to_dict(orient="records")
    )

    return {
        "intro": (
            f"Top {coactor_pairs} co-actor pairs based on shared movies (shared_movies_count)."
        ),
        "kpis": {"coactor_pairs": coactor_pairs, "top_pair": top_pair},
        "top_pairs_preview": preview,
    }


def build_insights(curated_dir: Path) -> Dict[str, Any]:
    movie_df = _read_table("curated_movie_overview", curated_dir)
    year_df = _read_table("curated_year_trends", curated_dir)
    genre_df = _read_table("curated_genre_stats", curated_dir)
    graph_df = _read_table("graph_insights_top_coactors", curated_dir)

    tables = {
        "curated_movie_overview": movie_df,
        "curated_year_trends": year_df,
        "curated_genre_stats": genre_df,
    }
    if graph_df is not None:
        tables["graph_insights_top_coactors"] = graph_df

    insights = {
        "overview": _build_overview(movie_df),
        "trends": _build_trends(year_df),
        "roi": _build_roi(movie_df, genre_df),
        "collab": _build_collab(graph_df),
        "data_quality": _data_quality_block(tables),
    }
    return insights


def main() -> None:
    ensure_directories()
    curated_dir = paths.curated_dir
    insights = build_insights(curated_dir)
    output_path = curated_dir / "insights.json"
    output_path.write_text(json.dumps(insights, indent=2, ensure_ascii=True), encoding="utf-8")
    print(f"Wrote {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build dashboard insights from curated tables.")
    args = parser.parse_args()
    main()
