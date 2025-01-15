CREATE TYPE film_stats AS (
	film TEXT,
	votes INTEGER,
	rating REAL,
	filmid TEXT
);

-- star: Average rating > 8.
-- good: Average rating > 7 and ≤ 8.
-- average: Average rating > 6 and ≤ 7.
-- bad: Average rating ≤ 6.
 
CREATE TYPE quality_class AS ENUM (
	'star', 'good', 'average', 'bad'
);

CREATE TABLE actors (
	actorid TEXT,
	actor TEXT,
	films film_stats[],
	quality_class quality_class,
	current_year INTEGER,
	is_active BOOLEAN,
	PRIMARY KEY(actor, actorid, current_year)
);

