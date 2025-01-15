-- Create Grouping Sets CTE
WITH grouping_sets AS (
	SELECT
		player_name,
		team_id,
		season,
		SUM(pts) AS total_points
	FROM
		games g
		INNER JOIN game_details gd ON g.game_id = gd.game_id
	WHERE
		pts IS NOT NULL
	GROUP BY 
		GROUPING SETS (
		(player_name, team_id, season),
		(player_name, team_id),
		(player_name, season),
		(team_id, season),
		(player_name),
		(team_id),
		(season),
		()
	)
)

-- Player & Team: Player who has most points for one team
SELECT
	player_name,
	team_id,
	total_points
FROM
	grouping_sets
WHERE
	player_name IS NOT NULL
	AND team_id IS NOT NULL
ORDER BY
	total_points DESC
LIMIT
	1

-- Player & Season: Player who has most points in a single season
SELECT
	player_name,
	season,
	total_points
FROM
	grouping_sets
WHERE
	player_name IS NOT NULL
	AND season IS NOT NULL
ORDER BY
	total_points DESC
LIMIT
	1