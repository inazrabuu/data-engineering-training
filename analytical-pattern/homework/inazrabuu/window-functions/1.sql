-- Most win in 90: Which team has the most wins in the stretch of 90 games
WITH games AS (
	SELECT
		game_date_est AS game_date,
		home_team_id AS team_id,
		home_team_wins AS win
	FROM
		games
	UNION ALL
	SELECT
		game_date_est AS game_date,
		visitor_team_id AS team_id,
		home_team_wins AS win
	FROM
		games
)
, rolling_wins AS (
	SELECT
		game_date,
		team_id,
		SUM(win) OVER (
			PARTITION BY
				team_id
			ORDER BY
				game_date ROWS BETWEEN 89 PRECEDING
				AND CURRENT ROW
		) AS win_count
	FROM
		games
)
SELECT
	rw.team_id,
	t.city || ' ' || t.nickname AS team_name,
	MAX(win_count) AS max_wins
FROM
	rolling_wins rw
	INNER JOIN teams t ON rw.team_id = t.team_id 
GROUP BY
	rw.team_id,
	team_name
ORDER BY
	max_wins DESC
