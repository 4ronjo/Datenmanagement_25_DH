"""
Central configuration for the movie ETL pipeline.
Provides base paths, input filenames, and shared parameters.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class DataPaths:
    raw_selected_dir: Path = BASE_DIR / "data" / "raw_selected" / "kaggle_movies"
    processed_dir: Path = BASE_DIR / "data" / "processed"
    curated_dir: Path = BASE_DIR / "data" / "curated"
    sql_dir: Path = BASE_DIR / "data" / "sql"
    neo4j_dir: Path = BASE_DIR / "data" / "neo4j"
    docs_dir: Path = BASE_DIR / "docs"


@dataclass(frozen=True)
class InputFiles:
    movies_metadata: str = "movies_metadata.csv"
    credits: str = "credits.csv"
    keywords: str = "keywords.csv"
    ratings_small: str = "ratings_small.csv"
    links_small: str = "links_small.csv"


@dataclass(frozen=True)
class Parameters:
    min_rating_count: int = 50
    max_cast_per_movie: int = 20


paths = DataPaths()
inputs = InputFiles()
params = Parameters()


def ensure_directories() -> None:
    """Create all required directories if they are missing."""
    for path in (
        paths.raw_selected_dir,
        paths.processed_dir,
        paths.curated_dir,
        paths.sql_dir,
        paths.neo4j_dir,
        paths.docs_dir,
    ):
        path.mkdir(parents=True, exist_ok=True)


__all__ = [
    "BASE_DIR",
    "paths",
    "inputs",
    "params",
    "ensure_directories",
    "DataPaths",
    "InputFiles",
    "Parameters",
]
