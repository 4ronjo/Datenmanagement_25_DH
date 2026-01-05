# Raw Data Profile

## movies_metadata
- Rows: 45466
- Columns: 24
- Column names: adult, belongs_to_collection, budget, genres, homepage, id, imdb_id, original_language, original_title, overview, popularity, poster_path, production_companies, production_countries, release_date, revenue, runtime, spoken_languages, status, tagline, title, video, vote_average, vote_count
- Dtypes: adult=object, belongs_to_collection=object, budget=object, genres=object, homepage=object, id=object, imdb_id=object, original_language=object, original_title=object, overview=object, popularity=object, poster_path=object, production_companies=object, production_countries=object, release_date=object, revenue=float64, runtime=float64, spoken_languages=object, status=object, tagline=object, title=object, video=object, vote_average=float64, vote_count=float64
- Missing values:
  - adult: 0 (0.0%)
  - belongs_to_collection: 40972 (90.12%)
  - budget: 0 (0.0%)
  - genres: 0 (0.0%)
  - homepage: 37684 (82.88%)
  - id: 0 (0.0%)
  - imdb_id: 17 (0.04%)
  - original_language: 11 (0.02%)
  - original_title: 0 (0.0%)
  - overview: 954 (2.1%)
  - popularity: 5 (0.01%)
  - poster_path: 386 (0.85%)
  - production_companies: 3 (0.01%)
  - production_countries: 3 (0.01%)
  - release_date: 87 (0.19%)
  - revenue: 6 (0.01%)
  - runtime: 263 (0.58%)
  - spoken_languages: 6 (0.01%)
  - status: 87 (0.19%)
  - tagline: 25054 (55.1%)
  - title: 6 (0.01%)
  - video: 6 (0.01%)
  - vote_average: 6 (0.01%)
  - vote_count: 6 (0.01%)
- Duplicate rows: 17
- Memory usage (bytes): 78431756

## credits
- Rows: 45476
- Columns: 3
- Column names: cast, crew, id
- Dtypes: cast=object, crew=object, id=int64
- Missing values:
  - cast: 0 (0.0%)
  - crew: 0 (0.0%)
  - id: 0 (0.0%)
- Duplicate rows: 37
- Memory usage (bytes): 268063855

## keywords
- Rows: 46419
- Columns: 2
- Column names: id, keywords
- Dtypes: id=int64, keywords=object
- Missing values:
  - id: 0 (0.0%)
  - keywords: 0 (0.0%)
- Duplicate rows: 987
- Memory usage (bytes): 8554685

## ratings_small
- Rows: 100004
- Columns: 4
- Column names: userId, movieId, rating, timestamp
- Dtypes: userId=int64, movieId=int64, rating=float64, timestamp=int64
- Missing values:
  - userId: 0 (0.0%)
  - movieId: 0 (0.0%)
  - rating: 0 (0.0%)
  - timestamp: 0 (0.0%)
- Duplicate rows: 0
- Memory usage (bytes): 3200260

## links_small
- Rows: 9125
- Columns: 3
- Column names: movieId, imdbId, tmdbId
- Dtypes: movieId=int64, imdbId=int64, tmdbId=float64
- Missing values:
  - movieId: 0 (0.0%)
  - imdbId: 0 (0.0%)
  - tmdbId: 13 (0.14%)
- Duplicate rows: 0
- Memory usage (bytes): 219132

## Join Checks
- Movies vs Credits: matches=45432, missing_in_credits=1, missing_in_movies=0
- Movies vs Keywords: matches=45432, missing_in_keywords=1, missing_in_movies=0
- Ratings vs Movies (different ID spaces): ratings=movieId (MovieLens), movies=id (TMDB)
- Direct overlap (should be low): 2830
- Mapping via links_small required: True
- Mapped matches (movieId -> tmdbId): 9082
- Movies without ratings after mapping: 36351
- Ratings without movies after mapping: 30
