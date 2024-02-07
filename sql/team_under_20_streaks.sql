WITH 
	totals_by_streak AS
	(
		SELECT 
			tgs.streak_id,
			tgs.team_id,
			SUM(tgs.game_streak_val) AS total_streak_val,
			COUNT(tgs.game_id) AS streak_game_count,
			MIN(lgl.game_date) AS first_game_date,
			MAX(lgl.game_date) AS last_game_date
		FROM 
			team_game_streaks tgs
		LEFT JOIN
			LEAGUE_GAME_LOGS lgl
			ON tgs.game_id = lgl.game_id
			AND tgs.team_id = lgl.team_id
			
		GROUP BY
			tgs.streak_id,
			tgs.team_id
		)
SELECT 
	tbs.streak_id,
	tbs.team_id, 
	lgl.team_name,
	tbs.total_streak_val,
	tbs.streak_game_count
FROM totals_by_streak tbs
LEFT JOIN
	LEAGUE_GAME_LOGS lgl
	ON tbs.team_id = lgl.team_id
WHERE total_streak_val  <=   -20
