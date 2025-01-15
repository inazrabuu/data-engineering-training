INSERT INTO host_activity_reduced
WITH today AS (
	SELECT
		HOST,
		event_time::DATE,		
		COUNT(1) AS hit_array,
		COUNT(DISTINCT user_id) AS unique_hit_array
	FROM
		events
	WHERE event_time::DATE = DATE('2023-01-31')
	AND user_id IS NOT NULL
	GROUP BY
		HOST,
		event_time::DATE
)
, yesterday AS (
	SELECT
		*
	FROM
		host_activity_reduced
	WHERE
		date_partition::DATE = DATE ('2023-01-30')
)
SELECT
	'2023-01-01'::DATE AS month_start,
	COALESCE(t.host, y.host) AS host,
	COALESCE(y.hit_array, 
			ARRAY_FILL(0::BIGINT, ARRAY[t.event_time::DATE - DATE('2023-01-01')]))
			|| ARRAY[t.hit_array] AS hit_array,
	COALESCE(y.unique_hit_array, 
			ARRAY_FILL(0::BIGINT, ARRAY[t.event_time::DATE - DATE('2023-01-01')]))
			|| ARRAY[t.unique_hit_array] AS unique_hit_array,
	COALESCE(t.event_time::DATE, y.date_partition::DATE + INTERVAL '1 day')::DATE AS date_partition
FROM
	yesterday y
	FULL OUTER JOIN today t ON y.host = t.host