import sqlite3
from sqlite3 import Error

NBA_DATA_DB = "nba-data.db"
DB_PATH = "backend/etl/data/" + NBA_DATA_DB
GAME_LOGS_TABLE = "LEAGUE_GAME_LOGS"
PBP_STATS_3_TABLE = "PLAY_BY_PLAYS_STATS_2"
PBP_LIVE_TABLE = "PLAY_BY_PLAYS_LIVE"
LIVE_PERSON_IDS_TABLE = "PLAY_BY_PLAYS_LIVE_PERSON_IDS"
LIVE_QUALIFIERS_TABLE = "PLAY_BY_PLAYS_LIVE_QUALIFIERS"
TEAM_SHOT_STREAKS_TABLE = "TEAM_SHOT_STREAKS"
PLAYER_SHOT_STREAKS_TABLE = "PLAYER_SHOT_STREAKS"
THREE_PT_SHOT_STREAKS_TABLE ="THREE_PT_SHOT_STREAKS_TABLE"
def dict_factory(cursor, row):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}

def create_connection(db_file=DB_PATH, use_custom_factory = False):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        
        if use_custom_factory:
            conn.row_factory = dict_factory
        else: conn.row_factory = sqlite3.Row
    except Error as e:
        print("Error connecting to: ",db_file, "\n", e )

    return conn