WITH cte AS (
	SELECT 
		team_id, 
		game_id,
		is_make, 
		SUM(
				CASE 
					WHEN is_make = prev THEN 0 
					ELSE 1 
				END) 
			OVER(ORDER BY game_date, event_num) AS grp
	 FROM 
	(
		SELECT 
			*, 
			LAG(is_make) 
				OVER(--PARTITION BY game_id
					ORDER BY game_date, event_num) 
			AS prev 
		FROM shots_sample
			) s
)
SELECT 
	team_id, 
	is_make,
	game_id,  
	COUNT(*) AS cnt
FROM cte
GROUP BY grp, team_id, game_id
ORDER BY grp;