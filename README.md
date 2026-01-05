# ETL + Dashboard + Graphdatenbank (Neo4j) – Movie & Collaboration Insights

Dieses Projekt implementiert einen vollständigen **ETL-Prozess** (Extract → Transform → Load) auf Basis eines umfangreichen Film-Datensatzes und stellt die Ergebnisse in einem **Dashboard** dar. Zusätzlich wird ein **Graph-Modell (Neo4j)** aufgebaut, um Beziehungen wie *Actor–Movie*, *Director–Movie* und *Movie–Genre/Keyword* zu analysieren.

---

## 1) Use Case / Zielsetzung

**Fragestellung (Business/BI):**
- Wie entwickeln sich **Filme** über die Jahre (Anzahl, Einnahmen, Ratings)?
- Welche **Genres** sind im Durchschnitt „erfolgreich“ (z. B. ROI / Revenue)?
- Welche **Schauspieler:innen** oder **Regisseur:innen** sind stark vernetzt (Kollaborationen)?
- Welche **Beziehungen** (Personen ↔ Filme ↔ Genres/Keywords/Companies) sind im Datensatz erkennbar?

**Warum dieser Use Case?**
- Der Datensatz ist **datenreich**, realistisch, enthält mehrere Tabellen und erfordert **Datenintegration** (klassischer ETL).
- Er eignet sich sowohl für **Dashboard/BI** (aggregierte Kennzahlen und Trends) als auch für **Graphanalysen** (Netzwerke und Beziehungen).

---

## 2) Datenquelle

Primärquelle: Kaggle Dataset **“The Movies Dataset”**  
Kaggle slug: `rounakbanik/the-movies-dataset`

Verwendete Dateien (Input):
- `movies_metadata.csv`
- `credits.csv`
- `keywords.csv`
- `ratings_small.csv`
- `links_small.csv` (Mapping MovieLens `movieId` → TMDB `id`)

> Wichtig: `ratings_small.csv` nutzt MovieLens IDs (`movieId`). Das Mapping auf TMDB-Filme erfolgt über `links_small.csv`.

Details zur Datenquelle stehen zusätzlich in: `data_sources.md`.

---

## 3) Architektur / Pipeline-Überblick

### ETL-Stufen
1. **Extract**
   - Download des Kaggle-Datasets (CSV) via Kaggle API
2. **Transform**
   - Profiling (Datenqualität, Missing Values, Dtypes, Duplikate, Join-Checks)
   - Bereinigung & Normalisierung in Dimensionen/Brücken (relational geeignet)
   - Aggregation von Ratings (pro Film)
3. **Load**
   - **Curated Layer** für Dashboard (analysefreundliche Tabellen)
   - **Neo4j Export** als Nodes/Relationships CSVs + Cypher Importskript

---

## 4) Projektstruktur (wichtigste Ordner)

data/
raw_selected/kaggle_movies/ # Input CSVs (aus Kaggle)
processed/ # bereinigte/normalisierte Tabellen (Parquet/CSV)
curated/ # dashboard-ready Tabellen (Parquet/CSV)
neo4j/ # Nodes & Relationships CSVs für Neo4j Import
docs/
raw_profile.md/.json # Profiling Ergebnis
transform_quality.md/.json # Transform-Qualitätsreport
pipeline_run.log # Lauf-Log
dashboard/
app.py # Streamlit Dashboard
requirements.txt # Dashboard-Abhängigkeiten
.streamlit/config.toml # optional: Theme/Layout
src/ (oder Projektroot, je nach Setup)
step01_profile_raw.py
step02_transform_processed.py
step03_build_curated.py
step04_export_neo4j.py
run_pipeline.py
config.py
neo4j_load.cypher
neo4j_queries.cypher
data_sources.md

yaml
Code kopieren

---

## 5) Voraussetzungen

- Python **3.10+**
- (Optional) Conda/venv empfohlen
- Kaggle API Zugriff (für Download)

### Kaggle API Token
1. Kaggle → Account → Settings → API → **Create New API Token**
2. `kaggle.json` ablegen:
   - macOS/Linux: `~/.kaggle/kaggle.json`
   - Windows: `C:\Users\<NAME>\.kaggle\kaggle.json`
3. macOS/Linux: `chmod 600 ~/.kaggle/kaggle.json`

---

## 6) Installation (Python)

Beispiel mit venv:
```bash
python -m venv .venv
source .venv/bin/activate   # macOS/Linux
# .venv\Scripts\activate    # Windows

pip install --upgrade pip
Falls die Pipeline eigene Requirements hat, entsprechend installieren (je nach Projektstand):

bash
Code kopieren
pip install pandas pyarrow numpy plotly streamlit kaggle
7) Pipeline ausführen (ETL End-to-End)
7.1 Extract (Download Kaggle)
bash
Code kopieren
python fetch_movies_kaggle.py
Stelle sicher, dass danach die Input-Dateien in data/raw_selected/kaggle_movies/ liegen
(oder passe Pfade entsprechend an, falls dein Fetch-Skript direkt dorthin schreibt).

7.2 ETL Pipeline (Profiling → Transform → Curated → Neo4j Export)
bash
Code kopieren
python run_pipeline.py
Optionen (falls implementiert):

bash
Code kopieren
python run_pipeline.py --skip-profile
python run_pipeline.py --skip-extract
python run_pipeline.py --skip-neo4j
python run_pipeline.py --format parquet
Erwartete Outputs:

docs/raw_profile.md + docs/raw_profile.json

docs/transform_quality.md + docs/transform_quality.json

data/processed/*.parquet (dim/bridge/fact Tabellen)

data/curated/*.parquet (dashboard-ready Tabellen)

data/neo4j/*.csv (nodes + relationships)

docs/pipeline_run.log

8) Neo4j: Import + Abfragen
8.1 CSVs in Neo4j import-Ordner kopieren
Neo4j Desktop:

Projekt → Datenbank → “Open Folder”

CSVs aus data/neo4j/ nach <neo4j-db>/import/ kopieren

8.2 Import ausführen
Im Neo4j Browser:

Inhalt von neo4j_load.cypher ausführen (Constraints + LOAD CSV + MERGE)

Danach neo4j_queries.cypher ausführen (Beispielabfragen)

8.3 Beispielabfragen (Ergebnisse für Bericht/Präsentation)
Top Movies nach Rating/Count

Häufigste Co-Actor Paare

Top Directors

Genres nach ROI

Keyword/Genre Verknüpfungen

Tipp für Abgabe: Speichere Screenshots von 2–3 Query-Resultaten + 1 Graph-Visualisierung.

9) Dashboard ausführen (Streamlit)
Installation (Dashboard-spezifisch):

bash
Code kopieren
pip install -r dashboard/requirements.txt
Start:

bash
Code kopieren
streamlit run dashboard/app.py
Das Dashboard liest standardmäßig aus data/curated/:

curated_movie_overview.(parquet|csv)

curated_genre_stats.(parquet|csv)

curated_year_trends.(parquet|csv)
Optional:

graph_insights_top_coactors.(parquet|csv) (für Graph Insights Seite)

Dashboard-Inhalte:

Filter: Jahr, Genre, Sprache, min. Rating-Count, Titel-Suche

KPIs: #Filme, ØRating, Sum Revenue, Median Budget

Trends: Filme pro Jahr, Revenue pro Jahr, optional Rating pro Jahr

ROI & Success: Budget vs Revenue, ROI nach Genre, ROI Verteilung

Graph Insights: Top Co-Actor-Paare (falls vorhanden)

10) Datenqualität & Reflexion (für Bericht)
Die wichtigsten Doku-Artefakte werden automatisch erzeugt:

docs/raw_profile.md → Profiling/Qualität der Rohdaten

docs/transform_quality.md → Qualität nach Transformation

Beispiele für reflektierbare Punkte:

Missing Values (z. B. budget/revenue)

Duplikate

Parsing von JSON-ähnlichen Spalten

ID-Mapping (MovieLens → TMDB über links_small.csv)

11) Troubleshooting (häufige Probleme)
ModuleNotFoundError: kaggle

bash
Code kopieren
python -m pip install kaggle
Neo4j kann CSV nicht laden

Stelle sicher, dass die CSVs im Neo4j import/ Ordner liegen

Prüfe Neo4j Settings bzgl. file:/// Import (je nach Version/Config)

Dashboard findet data/curated nicht

Stelle sicher, dass dashboard/app.py im dashboard/ Ordner liegt

Starte Streamlit aus dem Projekt-Root:

bash
Code kopieren
streamlit run dashboard/app.py
12) Ergebnis (Kurz)
Reproduzierbare ETL-Pipeline

Curated Tabellen für BI/Dashboard

Neo4j Graphmodell + Importskript + Query-Sammlung

Dashboard zur Visualisierung der wichtigsten Kennzahlen & Trends

