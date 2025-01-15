INSERT INTO user_device_cumulated (user_id, date_current, device_activity_datelist)
WITH join_with_row AS (
	SELECT
		e.user_id,
		e.device_id,
		e.event_time,
		e.host,
		d.browser_type,
		ROW_NUMBER() OVER (PARTITION BY e.user_id,
			e.device_id,
			e.event_time) AS row_num
	FROM
		events e
		JOIN devices d ON e.device_id = d.device_id
	WHERE
		user_id IS NOT NULL
)
, join_with_row_deduped AS (
	SELECT
		user_id,
		event_time::DATE,
		browser_type
	FROM
		join_with_row
	WHERE
		row_num = 1
	GROUP BY
		1,
		2,
		3
)
, cumulated_data AS (
	SELECT
		t1.user_id,
		t1.event_time::DATE,
		JSONB_OBJECT_AGG (t1.browser_type,
		ARRAY (
		SELECT
			event_time::DATE
		FROM
			join_with_row_deduped t2
		WHERE
			t2.user_id = t1.user_id
			AND t2.browser_type = t1.browser_type
			AND t2.event_time::DATE <= t1.event_time::DATE ORDER BY
				t2.event_time)) AS device_activity_datelist
	FROM
		join_with_row_deduped t1
	GROUP BY
		t1.user_id,
		t1.event_time::DATE
)
, yesterday AS (
	SELECT
		*
	FROM
		user_device_cumulated
	WHERE
		date_current = DATE('2022-12-31')
)
, today AS (
	SELECT
		*
	FROM
		cumulated_data
	WHERE
		event_time::DATE = DATE('2023-01-01')
)

SELECT
	COALESCE(t.user_id, y.user_id) AS user_id,
	COALESCE(t.event_time, y.date_current + INTERVAL '1 day')::DATE AS date_current,
	CASE 
		WHEN t.user_id IS NULL THEN y.device_activity_datelist
		WHEN y.user_id IS NULL THEN t.device_activity_datelist
		ELSE y.device_activity_datelist || t.device_activity_datelist
	END
FROM
	yesterday y
	FULL OUTER JOIN today t ON y.user_id = t.user_id
ON CONFLICT (user_id, date_current)
DO UPDATE SET
	device_activity_datelist = user_device_cumulated.device_activity_datelist || EXCLUDED.device_activity_datelist