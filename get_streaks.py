from db_methods import create_connection, DB_PATH, TEAM_SHOT_STREAKS_TABLE, PLAYER_SHOT_STREAKS_TABLE, THREE_PT_SHOT_STREAKS_TABLE
import pandas as pd
import numpy as np

pbp2_join_query_template = """
SELECT pbp2.GAME_ID, lgl.GAME_DATE, EVENTNUM, pbp2.HOMEDESCRIPTION,  pbp2.VISITORDESCRIPTION,  
PLAYER1_ID, PLAYER1_NAME, PLAYER1_TEAM_NICKNAME, PLAYER1_TEAM_ID ,
    CASE
        WHEN COALESCE(HOMEDESCRIPTION, '') <> '' THEN HOMEDESCRIPTION
        ELSE VISITORDESCRIPTION
    END AS DESCRIPTION
FROM  PLAY_BY_PLAYS_STATS_2 pbp2
LEFT JOIN LEAGUE_GAME_LOGS lgl ON pbp2.GAME_ID = lgl.GAME_ID
WHERE lgl.WL = "W" AND DESCRIPTION LIKE "%3PT%"
"""

def get_new_streak_val(prev_val, is_make, should_reset_streak_val):
    #If the shot is make, add one; if miss subtract
    streak_point = int(is_make) or -1
    added = prev_val + streak_point
    if should_reset_streak_val or abs(added) < abs(prev_val):
        new_val = streak_point
    else:
        new_val = added
    return new_val

def find_pbp2_miss_streaks(pbp2_join_df, streak_criteria, threshold = 10):
    streaks_list = []
    prev_shot_dict = {}

    for index, row in pbp2_join_df.iterrows():
        curr_shot_dict = {
                    "game_id": row["GAME_ID"],
                    "game_date":row["GAME_DATE"],
                    "event_num": row["EVENTNUM"],
                    "team_id": row["PLAYER1_TEAM_ID"],
                    "team_name": row["PLAYER1_TEAM_NICKNAME"],
                    "player_id": row["PLAYER1_ID"],
                    "player_name": row["PLAYER1_NAME"]
        }

        prev_game_streak_val = prev_shot_dict.get("game_streak_val", 0)
        is_new_subject = False 
        is_new_game = prev_shot_dict.get("game_id", 0) != curr_shot_dict.get("game_id")
        should_reset_game_streak_val = False

        is_new_subject =  curr_shot_dict.get(streak_criteria, None) !=  prev_shot_dict.get(streak_criteria, None)

        if is_new_subject or is_new_game: 
            should_reset_game_streak_val = True


        curr_shot_is_make = not row["DESCRIPTION"].startswith("MISS")
        curr_shot_dict["is_make"] = curr_shot_is_make
        curr_game_streak_val = get_new_streak_val(prev_game_streak_val, curr_shot_is_make, should_reset_game_streak_val)
        curr_shot_dict["game_streak_val"] = curr_game_streak_val

        is_different_shot_result = curr_shot_is_make != prev_shot_dict.get("is_make")
        should_assign_new_streak_id =  is_different_shot_result or is_new_subject
        
        if should_assign_new_streak_id:
            curr_shot_dict["streak_id"] = prev_shot_dict.get("streak_id", -1) + 1
        else:
            curr_shot_dict["streak_id"] = prev_shot_dict.get("streak_id", 0)

        if abs(curr_shot_dict["game_streak_val"]) >= threshold:
            streaks_list.append(curr_shot_dict)
        
        prev_shot_dict = curr_shot_dict
    streaks_df = pd.DataFrame.from_records(streaks_list)

    return streaks_df


def agg_streaks_by_game(df, streak_criteria):
    cols= {
        "player_id":["player_id", "player_name"],
        "team_id": ["team_id" , "team_name"]
    }
    game_streaks_df = df.copy()
    game_streaks_df["is_make"] = np.where(game_streaks_df["game_streak_val"] > 0, True, False)
    game_streaks_df["game_streak_val"] = game_streaks_df["game_streak_val"].abs()
    game_streaks_df = game_streaks_df.groupby(["streak_id","game_id", "is_make"] + cols[streak_criteria], as_index=False)["game_streak_val"].max()
    game_streaks_df["game_streak_val"] = np.where(game_streaks_df["is_make"] == False, game_streaks_df["game_streak_val"] *-1, game_streaks_df["game_streak_val"])
    print("games_df")
    print(game_streaks_df)
    return game_streaks_df

def agg_streak_counts(df):
    # Aggregate counts
    game_count_df = df.groupby('game_streak_val')['game_id'].count().reset_index(name='game_count')
    # print("game_ count df")
    # print(game_count_df)

    streak_count_df = df.groupby('streak_id')['game_streak_val'].sum().value_counts().reset_index(name="streak_count").sort_index()
    # print("streak count df")
    # print(streak_count_df)

    merged = streak_count_df.merge(game_count_df, on="game_streak_val", how="outer")

    # Rename columns
    merged = merged.rename(columns={'game_streak_val': 'streak_val'})
    #merged = merged["streak_val", "game_count", "streak_count"]
    merged["game_count"] = merged['game_count'].fillna(0)
    merged["game_count"] = merged["game_count"].astype('int')
    merged = merged.sort_values(by="streak_val")
    merged = merged.reset_index(drop=True)

    print("merged df")
    print(merged)
    return merged

def get_team_miss_streaks(threshold = 0, read_db_limit = 0):
    query = pbp2_join_query_template + "\nORDER BY pbp2.PLAYER1_TEAM_ID, GAME_DATE, pbp2.GAME_ID, EVENTNUM"
    query =  query + f"\nLIMIT {int(read_db_limit)}" if read_db_limit else query

    streaks_df = get_streaks(
        streak_criteria="team_id",
        query = query,
        game_streaks_csv_path= "exports/game_streaks/team_game_streaks.csv",
        streak_counts_csv_path= "exports/team_streak_counts.csv",
        save_table_name=TEAM_SHOT_STREAKS_TABLE,
        threshhold=threshold
    )

    return streaks_df

def get_player_miss_streaks(threshhold = 0 ,read_db_limit = 0 ):
    query = pbp2_join_query_template + "\nORDER BY PLAYER1_ID, GAME_DATE , pbp2.GAME_ID, EVENTNUM"
    query =  query + f"\nLIMIT {int(read_db_limit)}" if read_db_limit else query

    streaks_df = get_streaks(
        streak_criteria='player_id', 
        query = query, 
        game_streaks_csv_path= "exports/game_streaks/player_game_streaks.csv",
        streak_counts_csv_path="exports/player_streak_counts.csv", 
        save_table_name=PLAYER_SHOT_STREAKS_TABLE, 
        threshhold=threshhold)
    return streaks_df

def get_streaks(streak_criteria, query, game_streaks_csv_path, streak_counts_csv_path,  save_table_name ,threshhold=0):
    conn = create_connection(DB_PATH)
    print("Reading db")
    pbp2_join_df = pd.read_sql(query, conn)
    print(f"Getting streaks based on {streak_criteria}")
    streaks_df = find_pbp2_miss_streaks(pbp2_join_df, streak_criteria, threshhold)
    game_streaks_df= agg_streaks_by_game(streaks_df, streak_criteria)

    # Aggregate and save
    game_streaks_df.to_csv(game_streaks_csv_path, index=False)
    streak_counts_df = agg_streak_counts(game_streaks_df)
    streak_counts_df.to_csv(streak_counts_csv_path, index=False)

    streaks_df = streaks_df[["game_id", "event_num","team_id", "player_id", "streak_id", "game_streak_val"]]
    print("Saving to db.")
    res = streaks_df.to_sql(save_table_name, conn, if_exists= "replace", index= False)
    conn.close()
    print("Saved to db.")
    return streaks_df

get_team_miss_streaks()
get_player_miss_streaks()