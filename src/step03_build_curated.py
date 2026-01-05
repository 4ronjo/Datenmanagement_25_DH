"""
Build curated, dashboard-ready tables from processed data.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import pandas as pd

from src.config import ensure_directories, paths, params


def _read_table(name: str, fmt: str, directory: Path) -> pd.DataFrame:
    path = directory / f"{name}.{ 'parquet' if fmt == 'parquet' else 'csv'}"
    if fmt == "parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _write_table(df: pd.DataFrame, name: str, fmt: str, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    if fmt == "parquet":
        path = directory / f"{name}.parquet"
        df.to_parquet(path, index=False)
    else:
        path = directory / f"{name}.csv"
        df.to_csv(path, index=False)
    return path


def _aggregate_list(df: pd.DataFrame, key_col: str, value_col: str, top_n: int | None = None) -> pd.DataFrame:
    def join_values(values: pd.Series) -> str:
        unique_vals: List[str] = []
        for val in values:
            if pd.isna(val):
                continue
            if val not in unique_vals:
                unique_vals.append(val)
            if top_n and len(unique_vals) >= top_n:
                break
        return ", ".join(unique_vals)

    return (
        df.dropna(subset=[value_col])
        .groupby(key_col)[value_col]
        .apply(join_values)
        .reset_index()
    )


def main(output_format: str = "parquet") -> None:
    output_format = output_format.lower()
    ensure_directories()

    processed_dir = paths.processed_dir
    curated_dir = paths.curated_dir

    dim_movie = _read_table("dim_movie", output_format, processed_dir)
    bridge_movie_genre = _read_table("bridge_movie_genre", output_format, processed_dir)
    bridge_movie_company = _read_table("bridge_movie_company", output_format, processed_dir)
    bridge_movie_keyword = _read_table("bridge_movie_keyword", output_format, processed_dir)
    fact_ratings = _read_table("fact_movie_ratings_agg", output_format, processed_dir)
    ratings_for_stats = fact_ratings[fact_ratings["rating_count"] >= params.min_rating_count]

    genre_lists = _aggregate_list(bridge_movie_genre, "movie_id", "genre_name")
    company_lists = _aggregate_list(bridge_movie_company, "movie_id", "company_name", top_n=3)
    keyword_lists = _aggregate_list(bridge_movie_keyword, "movie_id", "keyword_name", top_n=10)

    curated_movie_overview = (
        dim_movie.merge(fact_ratings, on="movie_id", how="left")
        .merge(genre_lists, on="movie_id", how="left")
        .merge(company_lists, on="movie_id", how="left")
        .merge(keyword_lists, on="movie_id", how="left")
        .rename(
            columns={
                "genre_name": "genre_list",
                "company_name": "top_companies",
                "keyword_name": "keyword_list",
            }
        )
    )
    rating_mask = curated_movie_overview["rating_count"].fillna(0) >= params.min_rating_count
    curated_movie_overview["avg_rating_curated"] = curated_movie_overview["avg_rating"]
    curated_movie_overview.loc[~rating_mask, "avg_rating_curated"] = curated_movie_overview.loc[
        ~rating_mask, "vote_average"
    ]

    curated_genre_stats = (
        bridge_movie_genre.merge(dim_movie, on="movie_id", how="left")
        .merge(ratings_for_stats, on="movie_id", how="left")
        .groupby("genre_name")
        .agg(
            movie_count=("movie_id", "nunique"),
            avg_roi=("roi", "mean"),
            avg_rating=("avg_rating", "mean"),
        )
        .reset_index()
    )

    curated_year_trends = (
        dim_movie.merge(ratings_for_stats, on="movie_id", how="left")
        .dropna(subset=["release_year"])
        .groupby("release_year")
        .agg(
            movie_count=("movie_id", "nunique"),
            avg_budget=("budget", "mean"),
            avg_revenue=("revenue", "mean"),
            avg_rating=("avg_rating", "mean"),
        )
        .reset_index()
        .sort_values("release_year")
    )

    outputs = {
        "curated_movie_overview": curated_movie_overview,
        "curated_genre_stats": curated_genre_stats,
        "curated_year_trends": curated_year_trends,
    }
    for name, df in outputs.items():
        path = _write_table(df, name, output_format, curated_dir)
        print(f"- Wrote {name} to {path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build curated tables for dashboard consumption.")
    parser.add_argument(
        "--format",
        dest="output_format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Input/output format matching processed tables.",
    )
    args = parser.parse_args()
    main(output_format=args.output_format)