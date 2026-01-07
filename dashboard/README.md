# Streamlit Dashboard

This app visualizes the curated outputs of the ETL pipeline (movies, genres, trends, optional graph insights).

## Run
```bash
source .venv/bin/activate
pip install -r dashboard/requirements.txt
streamlit run dashboard/app.py
```

## Data inputs
The dashboard reads from `data/curated/`:
- `curated_movie_overview.(parquet|csv)`
- `curated_genre_stats.(parquet|csv)`
- `curated_year_trends.(parquet|csv)`
- optional: `graph_insights_top_coactors.(parquet|csv)`
- optional: `insights.json` (auto-generated text/KPIs)

If `insights.json` is missing or invalid, the app uses safe defaults.

## Notes
- Filters are applied to the curated movie table.
- The glossary under charts explains key terms (Revenue, ROI, etc.).
