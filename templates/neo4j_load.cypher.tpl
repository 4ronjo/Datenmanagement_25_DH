// Constraints
CREATE CONSTRAINT movie_id IF NOT EXISTS FOR (m:Movie) REQUIRE m.id IS UNIQUE;
CREATE CONSTRAINT person_id IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT genre_name IF NOT EXISTS FOR (g:Genre) REQUIRE g.name IS UNIQUE;
CREATE CONSTRAINT keyword_name IF NOT EXISTS FOR (k:Keyword) REQUIRE k.name IS UNIQUE;
CREATE CONSTRAINT company_name IF NOT EXISTS FOR (c:Company) REQUIRE c.name IS UNIQUE;

// --------------------
// Load nodes
// --------------------

// Movies
LOAD CSV WITH HEADERS FROM 'file:///nodes_movie.csv' AS row
CALL {
  WITH row
  MERGE (m:Movie {id: toInteger(row.`movie_id:ID(Movie)` )})
  SET m.title        = row.title,
      m.release_year = toInteger(row.release_year),
      m.budget       = toFloat(row.budget),
      m.revenue      = toFloat(row.revenue),
      m.avg_rating   = toFloat(row.avg_rating),
      m.rating_count = toInteger(row.rating_count)
} IN TRANSACTIONS OF 1000 ROWS;

// Persons
LOAD CSV WITH HEADERS FROM 'file:///nodes_person.csv' AS row
CALL {
  WITH row
  MERGE (p:Person {id: toInteger(row.`person_id:ID(Person)` )})
  SET p.name = row.name
} IN TRANSACTIONS OF 1000 ROWS;

// Genres
LOAD CSV WITH HEADERS FROM 'file:///nodes_genre.csv' AS row
CALL {
  WITH row
  MERGE (g:Genre {name: row.`name:ID(Genre)`})
} IN TRANSACTIONS OF 1000 ROWS;

// Keywords
LOAD CSV WITH HEADERS FROM 'file:///nodes_keyword.csv' AS row
CALL {
  WITH row
  MERGE (k:Keyword {name: row.`name:ID(Keyword)`})
} IN TRANSACTIONS OF 1000 ROWS;

// Companies
LOAD CSV WITH HEADERS FROM 'file:///nodes_company.csv' AS row
CALL {
  WITH row
  MERGE (c:Company {name: row.`name:ID(Company)`})
} IN TRANSACTIONS OF 1000 ROWS;

// --------------------
// Load relationships
// --------------------

// ACTED_IN (with properties)
LOAD CSV WITH HEADERS FROM 'file:///rel_ACTED_IN.csv' AS row
CALL {
  WITH row
  MATCH (p:Person {id: toInteger(row.`:START_ID(Person)`)})
  MATCH (m:Movie  {id: toInteger(row.`:END_ID(Movie)`)})
  MERGE (p)-[r:ACTED_IN]->(m)
  SET r.character = row.character,
      r.cast_order = toInteger(row.`cast_order:int`)
} IN TRANSACTIONS OF 1000 ROWS;

// DIRECTED
LOAD CSV WITH HEADERS FROM 'file:///rel_DIRECTED.csv' AS row
CALL {
  WITH row
  MATCH (p:Person {id: toInteger(row.`:START_ID(Person)`)})
  MATCH (m:Movie  {id: toInteger(row.`:END_ID(Movie)`)})
  MERGE (p)-[:DIRECTED]->(m)
} IN TRANSACTIONS OF 1000 ROWS;

// IN_GENRE
LOAD CSV WITH HEADERS FROM 'file:///rel_IN_GENRE.csv' AS row
CALL {
  WITH row
  MATCH (m:Movie {id: toInteger(row.`:START_ID(Movie)`)})
  MATCH (g:Genre {name: row.`:END_ID(Genre)`})
  MERGE (m)-[:IN_GENRE]->(g)
} IN TRANSACTIONS OF 1000 ROWS;

// HAS_KEYWORD
LOAD CSV WITH HEADERS FROM 'file:///rel_HAS_KEYWORD.csv' AS row
CALL {
  WITH row
  MATCH (m:Movie {id: toInteger(row.`:START_ID(Movie)`)})
  MATCH (k:Keyword {name: row.`:END_ID(Keyword)`})
  MERGE (m)-[:HAS_KEYWORD]->(k)
} IN TRANSACTIONS OF 1000 ROWS;

// PRODUCED
LOAD CSV WITH HEADERS FROM 'file:///rel_PRODUCED.csv' AS row
CALL {
  WITH row
  MATCH (c:Company {name: row.`:START_ID(Company)`})
  MATCH (m:Movie {id: toInteger(row.`:END_ID(Movie)`)})
  MERGE (c)-[:PRODUCED]->(m)
} IN TRANSACTIONS OF 1000 ROWS;
