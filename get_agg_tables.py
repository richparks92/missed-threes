import pandas as pd
from db_methods import create_connection
import numpy as np

load_folder_path = "exports/game_streaks"
save_folder_path = "exports"

def load_game_logs():
    conn = create_connection()
    query = "SELECT game_id, team_id, team_name, game_date FROM LEAGUE_GAME_LOGS"
    lgl = pd.read_sql_query(query, conn)
    lgl.columns = lgl.columns.str.lower()
    lgl["game_id"] = lgl["game_id"].astype("int")
    # print("league game logs")
    # print(lgl)
    return lgl



def get_over_20_miss_streaks():
    df = pd.read_csv(load_folder_path + "/team_game_streaks.csv")
    #print(df)
    df["game_id"]= df["game_id"].astype("int")
    lgl = load_game_logs()
    df = df.merge(lgl, on=["game_id", "team_id"])


    df = df.groupby("streak_id").agg(
        team_id =  pd.NamedAgg(column="team_id", aggfunc="first"),
        team_name = pd.NamedAgg(column="team_name_y", aggfunc="first"),
        total_streak_val=pd.NamedAgg(column="game_streak_val", aggfunc="sum"),
        streak_game_count=pd.NamedAgg(column="game_id", aggfunc="count"),
        first_game_date=pd.NamedAgg(column="game_date", aggfunc="min"),
        last_game_date=pd.NamedAgg(column="game_date", aggfunc="max")
    )
    df = df[df["total_streak_val"] < -20]
    df =df.sort_values(["total_streak_val"], ascending=False)
    print(df)
    df.to_csv(f"{save_folder_path}/over_20_miss_streaks.csv")
    return df

def get_over_37_misses():
    conn = create_connection()
    query = """
        SELECT 
            GAME_ID, 
            TEAM_ID, 
            TEAM_NAME,
            GAME_DATE,
            FG3M,
            FG3A,
            FG3A- FG3M AS FG3_MISSES,
            ROUND(FG3M *100.0/ FG3A , 2) AS FG3_PCT,
            CASE	
                WHEN GAME_ID = "0041700317"  THEN "X"
                END AS GAME_OF_INTEREST
            
        FROM LEAGUE_GAME_LOGS
        WHERE FG3_MISSES >= 37
        ORDER BY FG3_PCT 
"""
    df = pd.read_sql_query(query, conn)
    print(df)
    df.to_csv(f"{save_folder_path}/over_37_misses.csv")

get_over_20_miss_streaks()
#get_over_37_misses()