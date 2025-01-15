-- Lebron James 10+: How many games in a row where Lebron James score over 10 points a game
WITH streaks AS (
	SELECT
		g.game_date_est,
		gd.player_id,
		gd.player_name,
		gd.pts,
		ROW_NUMBER() OVER (
			PARTITION BY
				gd.player_id
			ORDER BY
				g.game_date_est
		) - ROW_NUMBER() OVER (
			PARTITION BY
				gd.player_id,
				(pts > 10)
			ORDER BY
				g.game_date_est
		) AS streak_group
	FROM
		game_details gd
		INNER JOIN games g ON gd.game_id = g.game_id
	WHERE
		player_name = 'LeBron James'
)
SELECT
	player_id,
	player_name,
	COUNT(player_id) AS streak_length
FROM
	streaks
GROUP BY
	player_id,
	player_name,
	streak_group
ORDER BY
	streak_length DESC