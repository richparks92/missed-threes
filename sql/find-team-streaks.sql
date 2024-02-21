WITH  team_shots_raw AS (
	SELECT 
		pbp2.GAME_ID AS game_id, 
		lgl.GAME_DATE AS game_date, 
		EVENTNUM as event_num,  
		PLAYER1_ID AS player_id,  
		PLAYER1_TEAM_ID  AS team_id,
		CASE
			WHEN COALESCE(HOMEDESCRIPTION, '') <> '' THEN HOMEDESCRIPTION
			ELSE VISITORDESCRIPTION
		END AS description
	FROM  PLAY_BY_PLAYS_STATS_2 pbp2
	LEFT JOIN LEAGUE_GAME_LOGS lgl ON pbp2.GAME_ID = lgl.GAME_ID
	WHERE lgl.WL = "W" AND DESCRIPTION LIKE "%3PT%"
	ORDER BY pbp2.PLAYER1_TEAM_ID, GAME_DATE, pbp2.GAME_ID, EVENTNUM
		)
,team_shots AS (
	SELECT 
		game_id,
		game_date,
		event_num,
		player_id,
		team_id,
		CASE WHEN description LIKE "MISS%" THEN False ELSE True END AS  is_make
	FROM
		team_shots_raw
)

,lags AS (
	SELECT 
		*, 
		LAG(is_make) 
			OVER(
				ORDER BY team_id, game_date, event_num) 
		AS prev 
	FROM team_shots
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