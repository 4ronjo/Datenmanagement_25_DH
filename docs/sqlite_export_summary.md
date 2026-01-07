# SQLite Export Summary

## Row counts
- bridge_movie_cast: 474165
- bridge_movie_company: 70464
- bridge_movie_crew: 464314
- bridge_movie_director: 49048
- bridge_movie_genre: 91015
- bridge_movie_keyword: 158680
- curated_genre_stats: 20
- curated_movie_overview: 45433
- curated_year_trends: 135
- dim_company: 23537
- dim_genre: 20
- dim_keyword: 19956
- dim_movie: 45433
- dim_person: 316323
- fact_movie_ratings_agg: 9053
- graph_insights_top_coactors: 200

## Indexes
- dim_movie(movie_id)
- dim_person(person_id)
- fact_movie_ratings_agg(movie_id)
- bridge_movie_cast(movie_id)
- bridge_movie_cast(person_id)
- bridge_movie_director(movie_id)
- bridge_movie_director(person_id)
- bridge_movie_genre(movie_id)
- bridge_movie_keyword(movie_id)
- bridge_movie_company(movie_id)
- curated_movie_overview(movie_id)
- curated_movie_overview(release_year)

## Data quality checks
- fact_movie_ratings_agg: 28 rows with movie_id not in dim_movie