--CREATE TABLE shots_sample ( game_id TEXT,  game_date TEXT,  event_num INT,  player_id TEXT,  team_id TEXT,  is_make INT);

WITH shots AS 
	(
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
		LIMIT 10000
		)
		

INSERT INTO shots_sample

SELECT 
	game_id,
	game_date,
	event_num,
	player_id,
	team_id,
	CASE WHEN description LIKE "MISS%" THEN False ELSE True END AS  is_make
FROM
	shots
;