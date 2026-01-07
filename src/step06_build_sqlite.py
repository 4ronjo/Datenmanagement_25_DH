"""
Build a SQLite database from processed and curated tables.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from src.config import ensure_directories, paths


def _collect_table_paths(directory: Path) -> Dict[str, Path]:
    tables: Dict[str, Path] = {}
    for ext in ("csv", "parquet"):
        for path in directory.glob(f"*.{ext}"):
            name = path.stem
            existing = tables.get(name)
            if existing is None:
                tables[name] = path
            else:
                # Prefer parquet over csv if both exist
                if existing.suffix == ".csv" and path.suffix == ".parquet":
                    tables[name] = path
    return tables


def _read_table(path: Path) -> pd.DataFrame:
    if path.suffix == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    cur = conn.execute(f"PRAGMA table_info({table})")
    return [row[1] for row in cur.fetchall()]


def _maybe_create_index(conn: sqlite3.Connection, table: str, column: str) -> bool:
    if not _table_exists(conn, table):
        return False
    if column not in _table_columns(conn, table):
        return False
    idx_name = f"idx_{table}_{column}"
    conn.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table}({column})")
    return True


def _write_summary(
    summary_path: Path,
    row_counts: Dict[str, int],
    index_notes: List[str],
    warnings: List[str],
) -> None:
    lines = ["# SQLite Export Summary", ""]
    lines.append("## Row counts")
    for name, count in sorted(row_counts.items()):
        lines.append(f"- {name}: {count}")
    lines.append("")
    lines.append("## Indexes")
    if index_notes:
        lines.extend([f"- {note}" for note in index_notes])
    else:
        lines.append("- (none)")
    lines.append("")
    lines.append("## Data quality checks")
    if warnings:
        lines.extend([f"- {note}" for note in warnings])
    else:
        lines.append("- No issues detected.")
    summary_path.write_text("\n".join(lines), encoding="utf-8")


def _missing_movie_id_check(conn: sqlite3.Connection, table: str) -> Tuple[str, int]:
    query = (
        f"SELECT COUNT(*) FROM {table} t "
        "LEFT JOIN dim_movie d ON t.movie_id = d.movie_id "
        "WHERE d.movie_id IS NULL"
    )
    count = conn.execute(query).fetchone()[0]
    return table, int(count)


def main() -> None:
    ensure_directories()
    processed_dir = paths.processed_dir
    curated_dir = paths.curated_dir
    sql_dir = paths.sql_dir
    docs_dir = paths.docs_dir

    sql_dir.mkdir(parents=True, exist_ok=True)
    db_path = sql_dir / "movies_etl.sqlite"
    if db_path.exists():
        db_path.unlink()

    processed_tables = _collect_table_paths(processed_dir)
    curated_tables = _collect_table_paths(curated_dir)

    table_paths = {**processed_tables, **curated_tables}

    row_counts: Dict[str, int] = {}
    index_notes: List[str] = []
    warnings: List[str] = []

    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        for name, path in sorted(table_paths.items()):
            if name == "insights":
                continue
            df = _read_table(path)
            df.to_sql(name, conn, if_exists="replace", index=False)
            row_counts[name] = int(len(df))
            print(f"[sqlite] wrote {name} ({len(df)} rows)")

        # Indexes (defensive)
        index_targets = [
            ("dim_movie", "movie_id"),
            ("dim_person", "person_id"),
            ("fact_movie_ratings_agg", "movie_id"),
            ("bridge_movie_cast", "movie_id"),
            ("bridge_movie_cast", "person_id"),
            ("bridge_movie_director", "movie_id"),
            ("bridge_movie_director", "person_id"),
            ("bridge_movie_genre", "movie_id"),
            ("bridge_movie_keyword", "movie_id"),
            ("bridge_movie_company", "movie_id"),
            ("curated_movie_overview", "movie_id"),
            ("curated_movie_overview", "release_year"),
        ]
        for table, column in index_targets:
            if _maybe_create_index(conn, table, column):
                index_notes.append(f"{table}({column})")

        # Data quality checks: movie_id coverage
        if _table_exists(conn, "dim_movie") and "movie_id" in _table_columns(conn, "dim_movie"):
            check_tables = [
                "fact_movie_ratings_agg",
                "bridge_movie_cast",
                "bridge_movie_director",
                "bridge_movie_genre",
                "bridge_movie_keyword",
                "bridge_movie_company",
            ]
            for table in check_tables:
                if _table_exists(conn, table) and "movie_id" in _table_columns(conn, table):
                    _, missing = _missing_movie_id_check(conn, table)
                    if missing > 0:
                        warnings.append(f"{table}: {missing} rows with movie_id not in dim_movie")

    summary_path = docs_dir / "sqlite_export_summary.md"
    _write_summary(summary_path, row_counts, index_notes, warnings)
    print(f"[sqlite] wrote {db_path}")
    print(f"[sqlite] summary {summary_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Build SQLite database from ETL outputs.")
    args = parser.parse_args()
    main()
