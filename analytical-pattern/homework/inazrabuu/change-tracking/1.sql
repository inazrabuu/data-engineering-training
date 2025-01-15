-- Track the state change for the players with these criterias:
-- - A player entering the league should be 'New'
-- - A player staying in the league should be 'Continued Playing'
-- - A player leaving the league should be 'Retired'
-- - A player that stays out of the league should be 'Stayed Retired'
-- - A player that comes out of retirement should be 'Returned from Retirement'

CREATE TYPE state_change AS ENUM (
	'New',
	'Continued Playing',
	'Retired',
	'Stayed Retired',
	'Returned From Retirement'
);

SELECT
	player_name,
	current_season,
	is_active,
	CASE
		WHEN is_active = TRUE
		AND LAG(is_active) OVER (
			PARTITION BY
				player_name
			ORDER BY
				current_season
		) = FALSE THEN 'Returned From Retirement'
		WHEN is_active = FALSE
		AND LAG(is_active) OVER (
			PARTITION BY
				player_name
			ORDER BY
				current_season
		) = TRUE THEN 'Retired'
		WHEN is_active = TRUE
		AND LAG(is_active) OVER (
			PARTITION BY
				player_name
			ORDER BY
				current_season
		) = TRUE THEN 'Continued Playing'
		WHEN is_active = FALSE
		AND LAG(is_active) OVER (
			PARTITION BY
				player_name
			ORDER BY
				current_season
		) = FALSE THEN 'Stayed Retired'
		ELSE 'New'
	END::state_change AS state_change
FROM
	players