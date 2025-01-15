
CREATE TYPE scd_type AS (
	quality_class quality_class,
	is_active boolean,
	start_date INTEGER,
	end_date INTEGER
);

INSERT INTO actors_history_scd
WITH last_year_scd AS (
	SELECT
		*
	FROM
		actors_history_scd
	WHERE
		end_date = 1975
), historical_scd AS (
	SELECT
		actor,
		quality_class,
		is_active,
		start_date,
		end_date
	FROM
		actors_history_scd
	WHERE
		end_date < 1975
), this_year_data AS (
	SELECT * FROM actors WHERE current_year = 1976
), unchanged_records AS (
	SELECT
		ts.actor,
		ts.quality_class,
		ts.is_active,
		ls.start_date,
		ts.current_year AS end_date
	FROM
		this_year_data ts
		JOIN last_year_scd ls ON ls.actor = ts.actor
	WHERE
		ts.quality_class = ls.quality_class
		AND ts.is_active = ls.is_active	
), changed_records AS (
	SELECT
		ts.actor,
		UNNEST(ARRAY [
	                     ROW(
	                         ls.quality_class,
	                         ls.is_active,
	                         ls.start_date,
	                         ls.end_date
	                         )::scd_type,
	                     ROW(
	                         ts.quality_class,
	                         ts.is_active,
	                         ts.current_year,
	                         ts.current_year
	                         )::scd_type
	                 ]) AS records
	FROM
		this_year_data ts
		LEFT JOIN last_year_scd ls ON ls.actor = ts.actor
	WHERE (ts.quality_class <> ls.quality_class
		OR ts.is_active <> ls.is_active)
), unnested_changed_records AS (
	SELECT
		actor,
		(records::scd_type).quality_class,
		(records::scd_type).is_active,
		(records::scd_type).start_date,
		(records::scd_type).end_date
	FROM
		changed_records
), new_records AS (
	SELECT
		ts.actor,
		ts.quality_class,
		ts.is_active,
		ts.current_year AS start_date,
		ts.current_year AS end_date
	FROM
		this_year_data ts
		LEFT JOIN last_year_scd ls ON ts.actor = ls.actor
	WHERE
		ls.actor IS NULL
)

SELECT * FROM (
	SELECT * FROM historical_scd
	
	UNION ALL 
	
	SELECT * FROM unchanged_records
	
	UNION ALL 
	
	SELECT * FROM unnested_changed_records
	
	UNION ALL 
	
	SELECT * FROM new_records
) AS incremental_scd ORDER BY actor, start_date