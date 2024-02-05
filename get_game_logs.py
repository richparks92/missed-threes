from nba_api.stats.endpoints.leaguegamelog import LeagueGameLog
import pandas as pd
from db_methods import GAME_LOGS_TABLE, DB_PATH, create_connection

def get_game_log_rows(conn):
    cursor = conn.cursor()
    try:
        game_log_rows = cursor.execute("SELECT DISTINCT GAME_ID,SEASON_ID FROM {}".format(GAME_LOGS_TABLE)).fetchall()
    except Exception as e:
        print("No game logs retrieved")
        return []
    return game_log_rows

def fetch_new_game_logs():
    conn = create_connection(DB_PATH)
    e_game_log_rows = get_game_log_rows(conn)
    e_game_log_ids = [x["GAME_ID"] for x in e_game_log_rows]

    season_types = ["Regular Season", "Playoffs"]


    for season in range(1950, 2024):
        for season_type in season_types:
            log = LeagueGameLog(season=str(season), season_type_all_star= season_type)
            res_dfs = log.get_data_frames()

            if len(res_dfs) == 1:
                df = res_dfs[0]
                df = df[~df["GAME_ID"].isin(e_game_log_ids)]
                if len(df)>0:
                    df["SEASON_TYPE"] = season_type
                    print(df)
                    res = df.to_sql(GAME_LOGS_TABLE, conn, if_exists= "append", index= False)
                else:
                    print(f"No new game logs for season {str(season)} {season_type}")
            elif len(res_dfs) == 0:
                print(f"No dataframes found for season {str(season)}")
            elif len(res_dfs)> 1:
                print(f"Multiple dataframes found for season {str(season)}")


    conn.close()

fetch_new_game_logs()