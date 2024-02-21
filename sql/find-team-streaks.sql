WITH lags AS (
		SELECT 
			*, 
			LAG(is_make) 
				OVER(
					ORDER BY team_id, game_date, event_num) 
			AS prev 
		FROM shots_sample
		)

, streaks AS (
	SELECT 
		team_id, 
		game_id,
		is_make, 
		SUM(
				CASE 
					WHEN is_make = prev THEN 0 
					ELSE 1 
				END) 
			OVER(ORDER BY team_id, game_date, event_num) AS streak_id
	 FROM 
		lags
)

SELECT 
	team_id, 
	is_make,
	game_id,  
	COUNT(*) AS cnt,
	streak_id
FROM streaks
GROUP BY streak_id, game_id
ORDER BY streak_id;