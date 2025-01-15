INSERT INTO host_cumulated
WITH today AS (
	SELECT
		host,
		event_time::DATE
	FROM
		events
	WHERE
		event_time::DATE = DATE ('2023-01-31')
	GROUP BY
		HOST,
		event_time::DATE
), yesterday AS (
	SELECT
		host,
		host_activity_datelist,
		date_current
	FROM
		host_cumulated
	WHERE
		date_current::DATE = DATE ('2023-01-30')
)
SELECT
	COALESCE(t.host, y.host) as host,
	COALESCE(t.event_time, y.date_current + INTERVAL '1 day')::DATE AS date_current,
	CASE 
		WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.event_time]
		WHEN y.host_activity_datelist IS NOT NULL THEN y.host_activity_datelist || ARRAY[t.event_time]
		ELSE y.host_activity_datelist
	END AS host_activity_datelist
FROM
	yesterday y
	FULL OUTER JOIN today t ON y.host = t.host