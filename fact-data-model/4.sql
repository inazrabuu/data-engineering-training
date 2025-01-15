
WITH extracted_cumulated AS (
	SELECT
		user_id,
		date_current,
		browser_type,
		active_dates
	FROM
		user_device_cumulated,
		JSONB_EACH(device_activity_datelist) AS unpacked (browser_type,
			active_dates) 
	ORDER BY date_current
)
, starter AS (
	SELECT
		uc.user_id,
		uc.browser_type,
		ARRAY(SELECT JSONB_ARRAY_ELEMENTS_TEXT(active_dates)::DATE) @> ARRAY[d.valid_date::DATE] AS is_active,
		EXTRACT(
			DAY FROM DATE('2023-01-31') - d.valid_date::TIMESTAMP
		) AS days_since
	FROM
		extracted_cumulated uc
		CROSS JOIN (
			SELECT
				generate_series('2023-01-01', '2023-03-31', INTERVAL '1 day')::DATE AS valid_date) d
	WHERE
		date_current = DATE('2023-01-31') 
)
SELECT
	user_id,
	SUM(
		CASE 
			WHEN is_active THEN POW(2, 32 - days_since)
			ELSE 0
		END
	)::BIGINT::BIT(32) AS datelist_int,
	DATE('2023-01-31') AS date
FROM
	starter
GROUP BY 
	user_id