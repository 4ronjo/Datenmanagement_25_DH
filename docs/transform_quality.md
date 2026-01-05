# Transform Quality

## Row Counts
- dim_movie: 45433
- dim_person: 316323
- dim_genre: 20
- dim_company: 23537
- dim_keyword: 19956
- bridge_movie_genre: 91015
- bridge_movie_company: 70464
- bridge_movie_cast: 474165
- bridge_movie_crew: 464314
- bridge_movie_director: 49048
- bridge_movie_keyword: 158680
- fact_movie_ratings_agg: 9053

## Coverage
- Movies without genre: 2442
- Movies without cast: 2415
- Movies without keywords: 14341

## Budget/Revenue
- budget == 0: 36553
- revenue == 0: 38032

## Ratings Mapping
- Matches via links_small (movieId -> tmdbId): 9025
- Movies without ratings after mapping: 36408
- Ratings without movie metadata: 28
