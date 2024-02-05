SELECT 
	pss.streak_id, 
	pss.game_id,  
	lgl.GAME_DATE,
	player_id, 
	streak_val
FROM
	PLAYER_SHOT_STREAKS pss
LEFT JOIN 
	LEAGUE_GAME_LOGS lgl
	ON
		pss.game_id = lgl.game_id
		