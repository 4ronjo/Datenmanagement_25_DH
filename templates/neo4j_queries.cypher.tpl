// 1) Top movies with robust ratings (avoid noise)
MATCH (m:Movie)
WHERE m.rating_count >= 50 AND m.avg_rating IS NOT NULL
RETURN m.title AS title, m.release_year AS year, m.avg_rating AS rating, m.rating_count AS votes
ORDER BY rating DESC, votes DESC
LIMIT 20;

// 2) Most frequent co-actors (FIX: deduplicate + filter to avoid OOM)
MATCH (p1:Person)-[r1:ACTED_IN]->(m:Movie)<-[r2:ACTED_IN]-(p2:Person)
WHERE p1.id < p2.id
  AND m.rating_count >= 50
  AND (r1.cast_order IS NULL OR r1.cast_order <= 10)
  AND (r2.cast_order IS NULL OR r2.cast_order <= 10)
RETURN p1.name AS actor1, p2.name AS actor2, count(*) AS collaborations
ORDER BY collaborations DESC
LIMIT 20;

// 3) Directors with highest average rating (FIX: films >= 3 + total_votes)
MATCH (d:Person)-[:DIRECTED]->(m:Movie)
WHERE m.rating_count >= 50 AND m.avg_rating IS NOT NULL
WITH d,
     avg(m.avg_rating) AS avg_rating,
     count(m) AS films,
     sum(m.rating_count) AS total_votes
WHERE films >= 3
RETURN d.name AS director, avg_rating, films, total_votes
ORDER BY avg_rating DESC, total_votes DESC
LIMIT 20;

// 4) Top genres by ROI (FIX: robust ROI by excluding tiny budgets)
MATCH (m:Movie)-[:IN_GENRE]->(g:Genre)
WHERE m.budget IS NOT NULL AND m.revenue IS NOT NULL
  AND m.budget > 0
  AND m.budget >= 100000
WITH g, ((m.revenue - m.budget) / m.budget) AS roi
RETURN g.name AS genre, avg(roi) AS avg_roi, count(*) AS movies
ORDER BY avg_roi DESC
LIMIT 15;

// 5) Keyword communities: popular keywords by movie count
MATCH (m:Movie)-[:HAS_KEYWORD]->(k:Keyword)
RETURN k.name AS keyword, count(m) AS movie_count
ORDER BY movie_count DESC
LIMIT 25;

// 6) Nice graph screenshot query: top rated movie (with enough votes) + neighborhood
MATCH (m:Movie)
WHERE m.rating_count >= 200 AND m.avg_rating IS NOT NULL
WITH m
ORDER BY m.avg_rating DESC
LIMIT 1
MATCH (p:Person)-[r:ACTED_IN]->(m)
WHERE r.cast_order IS NULL OR r.cast_order <= 10
OPTIONAL MATCH (m)-[:IN_GENRE]->(g:Genre)
OPTIONAL MATCH (m)-[:HAS_KEYWORD]->(k:Keyword)
RETURN m, p, g, k
LIMIT 100;
