WITH totals_by_streak AS
	(
		SELECT 
			streak_id,
			player_id,
			SUM(game_streak_val) AS total_streak_val
			is_make

		FROM 
			agg_player_streaks
		GROUP BY
			streak_id,
			player_id,
			is_make
		ORDER BY
			streak_id
		),
	(
		SELECT
			
	)
	
		
SELECT
	total_streak_val,
	COUNT(streak_id) AS streak_count,
	COUNT(DISTINCT(streak_id) ) AS streak_distinct
FROM
	totals_by_streak AS tbs
LEFT JOIN
	(
		SELECT
			
	
	)
GROUP BY
	total_streak_val