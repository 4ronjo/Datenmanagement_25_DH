"""
Export processed tables into Neo4j-friendly node and relationship CSVs.
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Dict

import pandas as pd

from src.config import ensure_directories, paths


def _read_table(name: str, fmt: str, directory: Path) -> pd.DataFrame:
    path = directory / f"{name}.{ 'parquet' if fmt == 'parquet' else 'csv'}"
    if fmt == "parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _write_csv(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _safe_numeric(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce").astype("Int64")


def main(input_format: str = "parquet") -> None:
    input_format = input_format.lower()
    ensure_directories()
    processed_dir = paths.processed_dir
    neo4j_dir = paths.neo4j_dir
    docs_dir = paths.docs_dir

    dim_movie = _read_table("dim_movie", input_format, processed_dir)
    dim_person = _read_table("dim_person", input_format, processed_dir)
    dim_genre = _read_table("dim_genre", input_format, processed_dir)
    dim_keyword = _read_table("dim_keyword", input_format, processed_dir)
    dim_company = _read_table("dim_company", input_format, processed_dir)
    bridge_movie_cast = _read_table("bridge_movie_cast", input_format, processed_dir)
    bridge_movie_crew = _read_table("bridge_movie_crew", input_format, processed_dir)
    bridge_movie_director = _read_table("bridge_movie_director", input_format, processed_dir)
    bridge_movie_genre = _read_table("bridge_movie_genre", input_format, processed_dir)
    bridge_movie_keyword = _read_table("bridge_movie_keyword", input_format, processed_dir)
    bridge_movie_company = _read_table("bridge_movie_company", input_format, processed_dir)
    fact_ratings = _read_table("fact_movie_ratings_agg", input_format, processed_dir)

    dim_movie["movie_id"] = _safe_numeric(dim_movie["movie_id"])
    dim_person["person_id"] = _safe_numeric(dim_person["person_id"])
    dim_movie = dim_movie.dropna(subset=["movie_id"])
    dim_person = dim_person.dropna(subset=["person_id"])

    movies_with_ratings = dim_movie.merge(fact_ratings, on="movie_id", how="left")
    nodes_movie = movies_with_ratings[
        [
            "movie_id",
            "title",
            "release_year",
            "budget",
            "revenue",
            "avg_rating",
            "rating_count",
        ]
    ].copy()
    nodes_movie.rename(columns={"movie_id": "movie_id:ID(Movie)"}, inplace=True)
    nodes_movie = nodes_movie.drop_duplicates(subset=["movie_id:ID(Movie)"])

    nodes_person = dim_person.rename(columns={"person_id": "person_id:ID(Person)", "name": "name"})
    nodes_person = nodes_person.drop_duplicates(subset=["person_id:ID(Person)"])
    nodes_genre = dim_genre.rename(columns={"genre_name": "name:ID(Genre)"})
    nodes_keyword = dim_keyword.rename(columns={"keyword_name": "name:ID(Keyword)"})
    nodes_company = dim_company.rename(columns={"company_name": "name:ID(Company)"})

    for df_id in [
        bridge_movie_cast,
        bridge_movie_crew,
        bridge_movie_director,
        bridge_movie_genre,
        bridge_movie_keyword,
        bridge_movie_company,
    ]:
        if "movie_id" in df_id.columns:
            df_id["movie_id"] = _safe_numeric(df_id["movie_id"])
        if "person_id" in df_id.columns:
            df_id["person_id"] = _safe_numeric(df_id["person_id"])

    bridge_movie_cast = bridge_movie_cast.dropna(subset=["movie_id", "person_id"])
    bridge_movie_director = bridge_movie_director.dropna(subset=["movie_id", "person_id"])
    bridge_movie_genre = bridge_movie_genre.dropna(subset=["movie_id", "genre_name"])
    bridge_movie_keyword = bridge_movie_keyword.dropna(subset=["movie_id", "keyword_name"])
    bridge_movie_company = bridge_movie_company.dropna(subset=["movie_id", "company_name"])

    rel_acted_in = bridge_movie_cast.rename(
        columns={
            "person_id": ":START_ID(Person)",
            "movie_id": ":END_ID(Movie)",
            "character": "character",
            "cast_order": "cast_order:int",
        }
    )[[":START_ID(Person)", ":END_ID(Movie)", "character", "cast_order:int"]]

    rel_directed = bridge_movie_director.rename(
        columns={"person_id": ":START_ID(Person)", "movie_id": ":END_ID(Movie)"}
    )[ [":START_ID(Person)", ":END_ID(Movie)"] ].drop_duplicates()

    rel_in_genre = bridge_movie_genre.rename(
        columns={ "movie_id": ":START_ID(Movie)", "genre_name": ":END_ID(Genre)"}
    )[ [":START_ID(Movie)", ":END_ID(Genre)"] ].drop_duplicates()

    rel_has_keyword = bridge_movie_keyword.rename(
        columns={"movie_id": ":START_ID(Movie)", "keyword_name": ":END_ID(Keyword)"}
    )[ [":START_ID(Movie)", ":END_ID(Keyword)"] ].drop_duplicates()

    rel_produced = bridge_movie_company.rename(
        columns={"company_name": ":START_ID(Company)", "movie_id": ":END_ID(Movie)"}
    )[ [":START_ID(Company)", ":END_ID(Movie)"] ].drop_duplicates()

    _write_csv(nodes_movie, neo4j_dir / "nodes_movie.csv")
    _write_csv(nodes_person, neo4j_dir / "nodes_person.csv")
    _write_csv(nodes_genre, neo4j_dir / "nodes_genre.csv")
    _write_csv(nodes_keyword, neo4j_dir / "nodes_keyword.csv")
    _write_csv(nodes_company, neo4j_dir / "nodes_company.csv")

    _write_csv(rel_acted_in, neo4j_dir / "rel_ACTED_IN.csv")
    _write_csv(rel_directed, neo4j_dir / "rel_DIRECTED.csv")
    _write_csv(rel_in_genre, neo4j_dir / "rel_IN_GENRE.csv")
    _write_csv(rel_has_keyword, neo4j_dir / "rel_HAS_KEYWORD.csv")
    _write_csv(rel_produced, neo4j_dir / "rel_PRODUCED.csv")

    summary = {
        "nodes": {
            "Movie": len(nodes_movie),
            "Person": len(nodes_person),
            "Genre": len(nodes_genre),
            "Keyword": len(nodes_keyword),
            "Company": len(nodes_company),
        },
        "relationships": {
            "ACTED_IN": len(rel_acted_in),
            "DIRECTED": len(rel_directed),
            "IN_GENRE": len(rel_in_genre),
            "HAS_KEYWORD": len(rel_has_keyword),
            "PRODUCED": len(rel_produced),
        },
        "top_cast_by_movie": (
            bridge_movie_cast.groupby("movie_id")["person_id"]
            .count()
            .sort_values(ascending=False)
            .head(5)
            .to_dict()
        ),
    }

    lines = ["# Neo4j Export Summary", ""]
    lines.append("## Nodes")
    for label, count in summary["nodes"].items():
        lines.append(f"- {label}: {count}")
    lines.append("")
    lines.append("## Relationships")
    for rel, count in summary["relationships"].items():
        lines.append(f"- {rel}: {count}")
    lines.append("")
    lines.append("## Top 5 Movies by Cast Count")
    for movie_id, count in summary["top_cast_by_movie"].items():
        lines.append(f"- movie_id {movie_id}: {count} cast entries")
    docs_dir.mkdir(parents=True, exist_ok=True)
    (docs_dir / "neo4j_export_summary.md").write_text("\n".join(lines), encoding="utf-8")
    print(f"Neo4j CSVs written to {neo4j_dir}")
    print("Summary written to docs/neo4j_export_summary.md")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export Neo4j import CSVs from processed tables.")
    parser.add_argument(
        "--format",
        dest="input_format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Input format to read processed tables.",
    )
    args = parser.parse_args()
    main(input_format=args.input_format)