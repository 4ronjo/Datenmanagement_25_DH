# Movie ETL, Dashboard & Neo4j Graph

End-to-end Pipeline auf Basis des Kaggle-Datasets **“The Movies Dataset”**: ETL → kuratierte Tables → Streamlit-Dashboard → Neo4j-Graph-Export mit Import-/Query-Skripten.

---

## Datenquelle
- Kaggle slug: `rounakbanik/the-movies-dataset`
- Genutzt: `movies_metadata.csv`, `credits.csv`, `keywords.csv`, `ratings_small.csv`, `links_small.csv`  
  (MovieLens → TMDB Mapping via `links_small.csv`)

## Setup
```bash
python -m venv .venv
source .venv/bin/activate            # Windows: .venv\Scripts\activate
pip install -r requirements.txt      # Kern-ETL
pip install -r dashboard/requirements.txt  # Dashboard (optional)
```

### Kaggle API Token
`kaggle.json` in `data/raw/` **oder** `~/.kaggle/` ablegen (Script erkennt beides) und Datei-Rechte ggf. 600 setzen. Alternativ env: `KAGGLE_USERNAME` / `KAGGLE_KEY`.

## Pipeline ausführen
```bash
python -m src.run_pipeline
# Optionen:
#   --skip-extract   # wenn data/raw_selected schon gefüllt ist
#   --skip-profile
#   --skip-neo4j
#   --format parquet|csv
```
Outputs:
- `data/raw_selected/kaggle_movies/` (aus Fetch)
- `data/processed/` (dim/bridge/fact)
- `data/curated/` (Dashboard-Tables)
- `data/neo4j/*.csv` (Import für Neo4j)
- `docs/neo4j_load.cypher`, `docs/neo4j_queries.cypher` (werden automatisch erzeugt)
- `docs/raw_profile.*`, `docs/transform_quality.*`, `docs/pipeline_run.log`

Einzelner Fetch (falls nötig):
```bash
python -m src.fetch_movies_kaggle
```

## Dashboard (Streamlit)
```bash
streamlit run dashboard/app.py
# Netzwerkeinsatz:
# streamlit run dashboard/app.py --server.address 0.0.0.0 --server.port 8501
```
Liest standardmäßig aus `data/curated/` (`curated_movie_overview`, `curated_genre_stats`, `curated_year_trends`; optional `graph_insights_top_coactors`). Hinweis im UI: Basis sind `ratings_small`/`links_small`.

## Neo4j
1) CSVs aus `data/neo4j/` in den Neo4j-Import-Ordner kopieren.  
2) Im Browser `:play` bzw. Editor:
   - `docs/neo4j_load.cypher` ausführen (Neo4j 5 kompatibel, `CALL { … } IN TRANSACTIONS`, Constraints, ACTED_IN-Properties inkl. `cast_order`/`character`).
   - `docs/neo4j_queries.cypher` ausführen (robuste Demo-Queries: deduplizierte Co-Actors, ROI-Filter, Directors mit Films>=3, etc.).

## SQLite (zusätzliches Artefakt)
Der Pipeline-Run erzeugt automatisch eine SQLite-DB:
- Datei: `data/sql/movies_etl.sqlite`
- Zusammenfassung: `docs/sqlite_export_summary.md`

Öffnen:
- GUI: DB Browser for SQLite
- Python:
```python
import sqlite3
conn = sqlite3.connect("data/sql/movies_etl.sqlite")
```

Beispiel-Queries:
```sql
-- Top Movies nach Rating (mit genug Votes)
SELECT title, release_year, avg_rating, rating_count
FROM curated_movie_overview
WHERE rating_count >= 50
ORDER BY avg_rating DESC, rating_count DESC
LIMIT 20;

-- ROI nach Genre
SELECT genre_name, avg_roi, movie_count
FROM curated_genre_stats
ORDER BY avg_roi DESC
LIMIT 15;

-- Häufigste Co-Actor Paare (falls vorhanden)
SELECT actor_1, actor_2, shared_movies_count
FROM graph_insights_top_coactors
ORDER BY shared_movies_count DESC
LIMIT 20;
```

## Projektstruktur (Auszug)
- `src/run_pipeline.py` – orchestriert Extract→Transform→Load→Neo4j
- `src/fetch_movies_kaggle.py` – Kaggle Download + Auswahl
- `src/step0x_*.py` – ETL Schritte (Profiling, Transform, Curated, Neo4j Export)
- `dashboard/app.py` – Streamlit-App
- `data/` – raw_selected, processed, curated, neo4j
- `docs/` – Profile, Qualität, Logs, Neo4j Cypher
- `templates/` – Cypher-Templates für Neo4j Load/Queries

## Hinweise
- Python 3.10+ empfohlen (entwickelt unter 3.14).  
- `ratings_small` ist bewusst gewählt, um Größe/Performance für Uni-Abgabe niedrig zu halten. Für volle Ratings `python -m src.fetch_movies_kaggle --include-full-ratings` und Pipeline neu laufen lassen.
