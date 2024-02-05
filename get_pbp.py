from nba_api.live.nba.endpoints.playbyplay import PlayByPlay as LivePBP
from nba_api.stats.endpoints.playbyplayv2 import PlayByPlayV2 as StatsPBP
import pandas as pd
from db_methods import create_connection, PBP_STATS_3_TABLE, PBP_LIVE_TABLE, GAME_LOGS_TABLE, LIVE_PERSON_IDS_TABLE, LIVE_QUALIFIERS_TABLE, DB_PATH
from time import sleep
import traceback
from get_game_logs import get_game_log_rows

# Get existing pbp rows and filter out
def format_season_id_as_int(s_id):
    season_int = int(s_id[1:5])
    return season_int

def explode_actions_df(actions_df, game_id, col_to_explode):
    explode_df = actions_df[[col_to_explode,"actionNumber"]].dropna()
    explode_df["gameId"] = game_id
    explode_df = explode_df[explode_df[col_to_explode].map(len) > 0]
    explode_df = explode_df.explode(col_to_explode)
    print(explode_df)
    return explode_df

def get_existing_stat_pbps(conn):
    try:
        cursor = conn.cursor()
        e_stats_pbp_rows = cursor.execute("SELECT DISTINCT GAME_ID FROM {}".format(PBP_STATS_3_TABLE)).fetchall()
        e_pbp_game_ids = [x["GAME_ID"] for x in e_stats_pbp_rows ]
        print(f"Number of existing play by play rows: {len(e_pbp_game_ids)}")
        return e_pbp_game_ids

    except Exception as e:
        print("No existing stats play by play rows retrieved:")
        print(e)
        print(traceback.format_exc())
        return []

def get_existing_live_pbps(conn):
    try:
        cursor = conn.cursor()
        e_live_pbp_rows = cursor.execute("SELECT DISTINCT gameId FROM {}".format(PBP_LIVE_TABLE)).fetchall()
        e_pbp_game_ids = [x["gameId"] for x in e_live_pbp_rows ]
        return e_pbp_game_ids

    except Exception as e:
        print("No existing live play by play rows retrieved:")
        print(e)
        print(traceback.format_exc())
        return []
    


def get_pbps_to_retrieve(conn, start_season):
    game_log_rows = get_game_log_rows(conn)
    pbp_stat_game_ids = get_existing_stat_pbps(conn)
    pbp_live_game_ids = get_existing_live_pbps(conn)
    all_pbp_game_ids = pbp_live_game_ids + pbp_stat_game_ids
    pbps_to_retrieve_gl_rows  = [x for x in game_log_rows if x["GAME_ID"] not in all_pbp_game_ids and format_season_id_as_int(x["SEASON_ID"]) >= start_season]
    return pbps_to_retrieve_gl_rows

def get_stat_pbp_from_row(conn, game_id):
    try:
        pbp_dict = StatsPBP(game_id=game_id).get_dict()
        if pbp_dict:
            row_set = pbp_dict.get("resultSets",{})[0].get("rowSet",[])
            columns = pbp_dict.get("resultSets",{})[0].get("headers",[])
            df = pd.DataFrame(row_set)
            df.columns = columns

            df.to_sql(PBP_STATS_3_TABLE, conn, if_exists= "append", index= False)
            print(f"Successfull saved game {game_id} to DB")
            return True
        else:
            return False
    except Exception as e:
        print(f"Could not retrieve stat row for game ID: { game_id}")
        print(e)
        print(traceback.format_exc())
        return False

def get_live_pbp_from_row(conn, game_id):
    try:
        print("(Using LIVE endpoint)")
        pbp_dict = LivePBP(game_id=game_id).get_dict()
        if pbp_dict:

            game_id = pbp_dict.get("game",{}).get("gameId", {})
            actions = pbp_dict.get("game",{}).get("actions",[])
            actions_df = pd.DataFrame.from_records(actions)
            
            pbp_live_df = actions_df.drop(["qualifiers","personIdsFilter"], axis=1)
            pbp_live_df["gameId"] = game_id
            pbp_live_cols = pbp_live_df.columns.to_list()

            pbp_live_cols.insert(0, pbp_live_cols.pop(pbp_live_cols.index("gameId")))
            pbp_live_df = pbp_live_df[pbp_live_cols]

            qualifiers_df = explode_actions_df(actions_df, game_id, "qualifiers")
            p_ids_df = explode_actions_df(actions_df, game_id, "personIdsFilter")
            print(p_ids_df)

            pbp_live_df.to_sql(PBP_LIVE_TABLE, conn, if_exists= "append", index= False)
            qualifiers_df.to_sql(LIVE_QUALIFIERS_TABLE, conn, if_exists= "append", index= False)
            p_ids_df.to_sql(LIVE_PERSON_IDS_TABLE, conn, if_exists= "append", index= False)
            return True
        else: 
            return False

    except Exception as e:
        print(f"Could not retrieve live row for game ID: {game_id}")
        print(e)
        print(traceback.format_exc())
        return False


def write_game_pbps(conn, pbps_to_retrieve_gl_rows, api_call_limit = 5):

    num_rows = len(pbps_to_retrieve_gl_rows)
    api_call_limit = 25000
    num_api_calls = 0
    sleep_after = 2500

    for n, row in enumerate(pbps_to_retrieve_gl_rows):

        if num_api_calls >= api_call_limit: 
            print("-- Reached success limit --")
            break
        if num_api_calls >= sleep_after:
            if num_api_calls%sleep_after == 0:
                print(f"Sleeping at {num_api_calls} API calls")
                sleep(300)
            if num_api_calls%(sleep_after * 4) == 0:
                sleep(900)
            

        season_id = row["SEASON_ID"]
        game_id = row["GAME_ID"]
        season_format = format_season_id_as_int(season_id)
        
        
        #No PBP available for before 1996
        if season_format < 1996: 
            continue
        
        print(f"Fetching game {num_api_calls+1} / {num_rows} ({season_format})")
        if season_format < 2023:
            is_success = get_stat_pbp_from_row(conn, game_id)
        elif season_format >= 2023:
            is_success = get_live_pbp_from_row(conn,game_id)
            
        num_api_calls+=1  

def fetch_new_pbps():
    conn = create_connection(DB_PATH)

    pbps_to_retrieve_gl_rows = get_pbps_to_retrieve(conn, start_season= 1996)
    write_game_pbps(conn, pbps_to_retrieve_gl_rows)
    conn.close()
