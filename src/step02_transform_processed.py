"""
Transform raw Kaggle movie files into normalized processed tables.
"""

from __future__ import annotations

import argparse
import json
import ast
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd

from src.config import ensure_directories, inputs, params, paths


def _load_csv_with_fallback(path: Path) -> pd.DataFrame:
    for enc in ("utf-8", "latin-1"):
        try:
            return pd.read_csv(path, encoding=enc, low_memory=False)
        except Exception:  # pragma: no cover - runtime safeguard
            continue
    raise RuntimeError(f"Failed to read {path} with utf-8 or latin-1")


def _parse_json_list(value: Any) -> list:
    if isinstance(value, list):
        return value
    if pd.isna(value):
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except Exception:
            try:
                parsed = ast.literal_eval(value)
                return parsed if isinstance(parsed, list) else []
            except Exception:
                # Attempt to coerce single-quote JSON style
                try:
                    coerced = value.replace("'", '"')
                    parsed = json.loads(coerced)
                    return parsed if isinstance(parsed, list) else []
                except Exception:
                    return []
    return []


def _safe_numeric(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    return numeric.astype("Int64")


def _write_output(df: pd.DataFrame, name: str, output_format: str, directory: Path) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    if output_format == "csv":
        path = directory / f"{name}.csv"
        df.to_csv(path, index=False)
    else:
        path = directory / f"{name}.parquet"
        df.to_parquet(path, index=False)
    return path


def _build_movies(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, pd.DataFrame]]:
    df = df.rename(columns={"id": "movie_id"}).copy()
    df["movie_id"] = _safe_numeric(df["movie_id"])
    df = df.dropna(subset=["movie_id"])

    numeric_cols = ["budget", "revenue", "runtime", "popularity", "vote_average", "vote_count"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "release_date" in df.columns:
        df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
        df["release_year"] = df["release_date"].dt.year.astype("Int64")
    else:
        df["release_year"] = pd.NA

    df = df.drop_duplicates(subset=["movie_id"], keep="first")

    df["profit"] = df.get("revenue", pd.Series(dtype="float64")) - df.get(
        "budget", pd.Series(dtype="float64")
    )
    df["roi"] = np.where(df.get("budget", 0) > 0, df.get("revenue", np.nan) / df["budget"], np.nan)

    movie_cols = [
        "movie_id",
        "title",
        "release_year",
        "original_language",
        "budget",
        "revenue",
        "runtime",
        "popularity",
        "vote_average",
        "vote_count",
        "profit",
        "roi",
    ]
    dim_movie = df[[c for c in movie_cols if c in df.columns]].copy()

    bridge_movie_genre_records: List[Dict[str, Any]] = []
    bridge_movie_company_records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        movie_id = row["movie_id"]
        for genre in _parse_json_list(row.get("genres")):
            name = genre.get("name")
            if name:
                bridge_movie_genre_records.append({"movie_id": movie_id, "genre_name": name})
        for company in _parse_json_list(row.get("production_companies")):
            name = company.get("name")
            if name:
                bridge_movie_company_records.append({"movie_id": movie_id, "company_name": name})

    bridge_movie_genre = pd.DataFrame(
        bridge_movie_genre_records, columns=["movie_id", "genre_name"]
    )
    bridge_movie_company = pd.DataFrame(
        bridge_movie_company_records, columns=["movie_id", "company_name"]
    )

    dim_genre = pd.DataFrame({"genre_name": bridge_movie_genre["genre_name"].dropna().unique()})
    dim_company = pd.DataFrame({"company_name": bridge_movie_company["company_name"].dropna().unique()})

    return dim_movie, {
        "bridge_movie_genre": bridge_movie_genre,
        "bridge_movie_company": bridge_movie_company,
        "dim_genre": dim_genre,
        "dim_company": dim_company,
    }


def _build_credits(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    df = df.rename(columns={"id": "movie_id"}).copy()
    df["movie_id"] = _safe_numeric(df["movie_id"])
    df = df.dropna(subset=["movie_id"])

    cast_rows: List[Dict[str, Any]] = []
    crew_rows: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        movie_id = row["movie_id"]
        cast_list = _parse_json_list(row.get("cast"))
        crew_list = _parse_json_list(row.get("crew"))

        cast_list = sorted(
            (c for c in cast_list if isinstance(c, dict)),
            key=lambda x: x.get("order", 0),
        )[: params.max_cast_per_movie]
        for member in cast_list:
            cast_rows.append(
                {
                    "movie_id": movie_id,
                    "person_id": member.get("id"),
                    "person_name": member.get("name"),
                    "character": member.get("character"),
                    "cast_order": member.get("order"),
                }
            )

        for member in crew_list:
            crew_rows.append(
                {
                    "movie_id": movie_id,
                    "person_id": member.get("id"),
                    "person_name": member.get("name"),
                    "job": member.get("job"),
                    "department": member.get("department"),
                }
            )

    bridge_movie_cast = pd.DataFrame(
        cast_rows, columns=["movie_id", "person_id", "person_name", "character", "cast_order"]
    )
    bridge_movie_crew = pd.DataFrame(
        crew_rows, columns=["movie_id", "person_id", "person_name", "job", "department"]
    )

    if not bridge_movie_crew.empty:
        bridge_movie_crew["job"] = bridge_movie_crew["job"].fillna("").astype(str)
    bridge_movie_director = bridge_movie_crew.loc[
        bridge_movie_crew["job"].str.lower() == "director", ["movie_id", "person_id", "person_name"]
    ].copy()

    for df_numeric in (bridge_movie_cast, bridge_movie_crew, bridge_movie_director):
        if "person_id" in df_numeric.columns:
            df_numeric["person_id"] = _safe_numeric(df_numeric["person_id"])
        if "movie_id" in df_numeric.columns:
            df_numeric["movie_id"] = _safe_numeric(df_numeric["movie_id"])

    persons = pd.concat(
        [
            bridge_movie_cast[["person_id", "person_name"]],
            bridge_movie_crew[["person_id", "person_name"]],
        ],
        ignore_index=True,
    )
    persons = persons.dropna(subset=["person_id"]).drop_duplicates(subset=["person_id"])
    dim_person = persons.rename(columns={"person_name": "name"})
    dim_person["person_id"] = _safe_numeric(dim_person["person_id"])

    return {
        "bridge_movie_cast": bridge_movie_cast,
        "bridge_movie_crew": bridge_movie_crew,
        "bridge_movie_director": bridge_movie_director,
        "dim_person": dim_person,
    }


def _build_keywords(df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    df = df.rename(columns={"id": "movie_id"}).copy()
    df["movie_id"] = _safe_numeric(df["movie_id"])
    df = df.dropna(subset=["movie_id"])

    keyword_rows: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        for kw in _parse_json_list(row.get("keywords")):
            name = kw.get("name")
            if name:
                keyword_rows.append({"movie_id": row["movie_id"], "keyword_name": name})

    bridge_movie_keyword = pd.DataFrame(keyword_rows, columns=["movie_id", "keyword_name"])
    dim_keyword = pd.DataFrame(
        {"keyword_name": bridge_movie_keyword["keyword_name"].dropna().unique()}
    )
    return {
        "bridge_movie_keyword": bridge_movie_keyword,
        "dim_keyword": dim_keyword,
    }


def _build_ratings(
    ratings_df: pd.DataFrame, links_df: pd.DataFrame, movie_ids: Iterable[int]
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    ratings_df = ratings_df.rename(columns={"movieId": "movieId"}).copy()
    ratings_df["movieId"] = _safe_numeric(ratings_df["movieId"])
    ratings_df = ratings_df.dropna(subset=["movieId"])

    links_df = links_df.rename(columns={"movieId": "movieId", "tmdbId": "tmdbId"}).copy()
    links_df["movieId"] = _safe_numeric(links_df["movieId"])
    links_df["tmdbId"] = _safe_numeric(links_df["tmdbId"])
    links_df = links_df.dropna(subset=["movieId", "tmdbId"])

    ratings_with_tmdb = ratings_df.merge(links_df, on="movieId", how="left")
    ratings_with_tmdb = ratings_with_tmdb.rename(columns={"tmdbId": "movie_id"})
    ratings_with_tmdb = ratings_with_tmdb.dropna(subset=["movie_id"])

    ratings_with_tmdb["movie_id"] = _safe_numeric(ratings_with_tmdb["movie_id"])
    agg = ratings_with_tmdb.groupby("movie_id").agg(
        avg_rating=("rating", "mean"), rating_count=("rating", "count")
    )
    agg = agg.reset_index()

    movie_id_set = set(_safe_numeric(pd.Series(movie_ids)).dropna())
    agg_ids = set(agg["movie_id"].dropna())
    matching = len(agg_ids & movie_id_set)
    log = {
        "matches_tmdb": matching,
        "missing_in_ratings": len(movie_id_set - agg_ids),
        "missing_in_movies": len(agg_ids - movie_id_set),
        "key_column": "movie_id (tmdbId via links_small)",
    }
    return agg, log


def _quality_report(
    outputs: Dict[str, pd.DataFrame],
    dim_movie: pd.DataFrame,
    bridge_movie_genre: pd.DataFrame,
    bridge_movie_cast: pd.DataFrame,
    bridge_movie_keyword: pd.DataFrame,
    ratings_log: Dict[str, Any],
) -> Dict[str, Any]:
    movie_ids = set(dim_movie["movie_id"].dropna())
    without_genre = movie_ids - set(bridge_movie_genre["movie_id"].dropna())
    without_cast = movie_ids - set(bridge_movie_cast["movie_id"].dropna())
    without_keywords = movie_ids - set(bridge_movie_keyword["movie_id"].dropna())

    return {
        "row_counts": {name: int(len(df)) for name, df in outputs.items()},
        "movies_without_genre": len(without_genre),
        "movies_without_cast": len(without_cast),
        "movies_without_keywords": len(without_keywords),
        "budget_zero": int((dim_movie.get("budget", pd.Series(dtype=float)) == 0).sum()),
        "revenue_zero": int((dim_movie.get("revenue", pd.Series(dtype=float)) == 0).sum()),
        "ratings_mapping": ratings_log,
    }


def _quality_markdown(report: Dict[str, Any]) -> str:
    lines = ["# Transform Quality", ""]
    lines.append("## Row Counts")
    for name, count in report["row_counts"].items():
        lines.append(f"- {name}: {count}")
    lines.append("")
    lines.append("## Coverage")
    lines.append(f"- Movies without genre: {report['movies_without_genre']}")
    lines.append(f"- Movies without cast: {report['movies_without_cast']}")
    lines.append(f"- Movies without keywords: {report['movies_without_keywords']}")
    lines.append("")
    lines.append("## Budget/Revenue")
    lines.append(f"- budget == 0: {report['budget_zero']}")
    lines.append(f"- revenue == 0: {report['revenue_zero']}")
    lines.append("")
    lines.append("## Ratings Mapping")
    ratings = report["ratings_mapping"]
    lines.append(
        f"- Matches via links_small (movieId -> tmdbId): {ratings.get('matches_tmdb', 'n/a')}"
    )
    lines.append(
        f"- Movies without ratings after mapping: {ratings.get('missing_in_ratings', 'n/a')}"
    )
    lines.append(
        f"- Ratings without movie metadata: {ratings.get('missing_in_movies', 'n/a')}"
    )
    lines.append("")
    return "\n".join(lines)


def main(output_format: str = "parquet") -> None:
    output_format = output_format.lower()
    if output_format not in {"parquet", "csv"}:
        raise ValueError("output_format must be 'parquet' or 'csv'")

    ensure_directories()
    raw_dir = paths.raw_selected_dir
    processed_dir = paths.processed_dir
    docs_dir = paths.docs_dir

    print("Transforming raw files from", raw_dir, "to", processed_dir, f"({output_format})")
    movies_df = _load_csv_with_fallback(raw_dir / inputs.movies_metadata)
    credits_df = _load_csv_with_fallback(raw_dir / inputs.credits)
    keywords_df = _load_csv_with_fallback(raw_dir / inputs.keywords)
    ratings_df = _load_csv_with_fallback(raw_dir / inputs.ratings_small)
    links_df = _load_csv_with_fallback(raw_dir / inputs.links_small)

    dim_movie, movie_related = _build_movies(movies_df)
    credits_related = _build_credits(credits_df)
    keyword_related = _build_keywords(keywords_df)
    fact_movie_ratings_agg, ratings_log = _build_ratings(
        ratings_df, links_df, dim_movie["movie_id"].dropna().unique().tolist()
    )

    outputs: Dict[str, pd.DataFrame] = {
        "dim_movie": dim_movie,
        "dim_person": credits_related["dim_person"],
        "dim_genre": movie_related["dim_genre"],
        "dim_company": movie_related["dim_company"],
        "dim_keyword": keyword_related["dim_keyword"],
        "bridge_movie_genre": movie_related["bridge_movie_genre"],
        "bridge_movie_company": movie_related["bridge_movie_company"],
        "bridge_movie_cast": credits_related["bridge_movie_cast"],
        "bridge_movie_crew": credits_related["bridge_movie_crew"],
        "bridge_movie_director": credits_related["bridge_movie_director"],
        "bridge_movie_keyword": keyword_related["bridge_movie_keyword"],
        "fact_movie_ratings_agg": fact_movie_ratings_agg,
    }

    saved_paths = {}
    for name, df in outputs.items():
        saved_paths[name] = _write_output(df, name, output_format, processed_dir)
        print(f"- Wrote {name} to {saved_paths[name]}")

    quality = _quality_report(
        outputs,
        dim_movie,
        movie_related["bridge_movie_genre"],
        credits_related["bridge_movie_cast"],
        keyword_related["bridge_movie_keyword"],
        ratings_log,
    )
    quality_markdown = _quality_markdown(quality)

    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "transform_quality.json").write_text(json.dumps(quality, indent=2), encoding="utf-8")
    (docs_dir / "transform_quality.md").write_text(quality_markdown, encoding="utf-8")
    print("Quality report written to docs/transform_quality.{json,md}")
    print("Ratings key log:", ratings_log)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Transform raw Kaggle movie data.")
    parser.add_argument(
        "--format",
        dest="output_format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Output format for processed tables.",
    )
    args = parser.parse_args()
    main(output_format=args.output_format)