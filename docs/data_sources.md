# Data Sources

- Kaggle: `rounakbanik/the-movies-dataset`
  - Downloaded via `src/fetch_movies_kaggle.py` into `data/raw_selected/kaggle_movies/`
  - Raw files used: `movies_metadata.csv`, `credits.csv`, `keywords.csv`, `ratings_small.csv`, `links_small.csv`
- Credentials: `data/raw/kaggle.json` (repo-local; keep out of VCS)
- IDs:
  - TMDB movie id: `movies_metadata.id`
  - MovieLens movie id: `ratings_small.movieId`
  - Mapping: `links_small.movieId` -> `links_small.tmdbId`
- Umfang (grobe Größenordnung): `movies_metadata`/`credits`/`keywords` ca. 45k Zeilen, `ratings_small` ca. 100k Zeilen.
- Quelle/Lizenz: öffentliches Kaggle-Dataset (The Movies Dataset, TMDB-Quelle) – für Lehre/Analyse gedacht; respektiert TMDB Terms.
- Warum dieser Datensatz: kombiniert Metadaten (Titel, Jahr, Budget/Revenue), reichhaltige Beziehungen (Genres, Companies, Cast/Crew, Keywords) und Nutzer-Ratings (MovieLens) → eignet sich für ETL, Dashboarding und Graph-Analysen.