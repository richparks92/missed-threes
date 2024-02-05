WITH 
	totals_by_streak AS
	(
		SELECT 
			streak_id,
			team_id,
			SUM(game_streak_val) AS total_streak_val
		FROM 
			team_game_streaks
		GROUP BY
			streak_id,
			player_id
		),
		
	game_streak_counts AS
		(
			SELECT
				game_streak_val,
				COUNT(game_id) AS game_streak_count
			FROM team_game_streaks
			GROUP BY
				game_streak_val
		)
		
select total_streak_val, streak_count, gsc.game_streak_count

FROM(	
	SELECT
		total_streak_val,
		COUNT(streak_id) AS streak_count
		
	FROM
		totals_by_streak AS tbs

	GROUP BY
		total_streak_val
		
		)  AS sc
LEFT JOIN
	game_streak_counts gsc
	ON sc.total_streak_val = gsc.game_streak_val