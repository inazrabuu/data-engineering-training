INSERT INTO actors_history_scd
WITH streak_started AS (
	SELECT
		actor,
		current_year,
		quality_class,
		is_active,
		LAG(quality_class, 1) 
			OVER (PARTITION BY actor ORDER BY current_year) <> quality_class
		OR 
		LAG(quality_class, 1) OVER (PARTITION BY actor ORDER BY current_year) IS NULL 
		AS did_change
	FROM
		actors
), streak_identified AS (
	SELECT
		actor,
		quality_class,
		current_year,
		is_active,
		SUM(
			CASE WHEN did_change THEN
				1
			ELSE
				0
			END) OVER (PARTITION BY actor ORDER BY current_year) AS streak_identifier
	FROM
		streak_started
), aggregated AS (
	SELECT
		actor,
		quality_class,
		is_active,
		streak_identifier,
		MIN(current_year) AS start_date,
		MAX(current_year) AS end_date
	FROM
		streak_identified
	GROUP BY
		1,
		2,
		3,
		4
)

SELECT
	actor,
	quality_class,
	is_active,
	streak_identifier,
	start_date,
	end_date
FROM
	aggregated