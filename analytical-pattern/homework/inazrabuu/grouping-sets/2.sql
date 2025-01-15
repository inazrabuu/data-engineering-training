-- Team: Which team has won the most games
WITH
	home_team_win AS (
		SELECT
			game_date_est AS game_date,
			home_team_id AS team_id
		FROM
			games
		WHERE
			home_team_wins = 1
	),
	guest_team_win AS (
		SELECT
			game_date_est AS game_date,
			visitor_team_id AS team_id
		FROM
			games
		WHERE
			home_team_wins = 0
	),
	all_winners AS (
		SELECT
			game_date,
			team_id
		FROM
			home_team_win
		UNION ALL
		SELECT
			game_date,
			team_id
		FROM
			guest_team_win
	)
SELECT
	t.team_id,
	t.city || ' ' || t.nickname AS team_name,
	COUNT(t.team_id) AS total_win
FROM
	all_winners aw
	INNER JOIN teams t ON aw.team_id = t.team_id
GROUP BY
	t.team_id, team_name
ORDER BY
	total_win DESC
LIMIT 
	1