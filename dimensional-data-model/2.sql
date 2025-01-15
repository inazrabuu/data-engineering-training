INSERT INTO actors

-- CTE to select data about lastyear
WITH lastyear AS (
    SELECT
		actorid,
		actor,
		current_year,
		films,
		quality_class,
		is_active
	FROM
		actors
	WHERE
		current_year = 1970
),

-- CTE to select data about thisyear
thisyear AS (
	SELECT
		actorid,
		actor,
		year AS current_year,
		ARRAY_AGG(ROW (film, votes, rating, filmid)::film_stats) AS films,
		CASE WHEN AVG(rating) > 8 THEN
			'star'
		WHEN AVG(rating) > 7
			AND AVG(rating) <= 8 THEN
			'good'
		WHEN AVG(rating) > 6
			AND AVG(rating) <= 7 THEN
			'average'
		WHEN AVG(rating) <= 6 THEN
			'bad'
		ELSE
			NULL END::quality_class AS quality_class
	FROM
		actor_films
	WHERE
		year = 1971
	GROUP BY
		actorid,
		actor,
		year
)

SELECT
	COALESCE(l.actorid, t.actorid) AS actorid,
	COALESCE(l.actor, t.actor) AS actor,
	CASE 
		WHEN l.films IS NULL THEN t.films
		WHEN l.films IS NOT NULL THEN l.films || t.films 
		ELSE l.films
	END AS films,
	COALESCE(t.quality_class, l.quality_class) AS quality_class,
	COALESCE(t.current_year, l.current_year + 1) AS current_year,
	CASE
		WHEN t.films IS NOT NULL AND ARRAY_LENGTH(t.films, 1) > 0 THEN TRUE
		ELSE FALSE
	END AS is_active
FROM
	lastyear l
	FULL OUTER JOIN thisyear t ON l.actorid = t.actorid;