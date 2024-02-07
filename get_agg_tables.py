import pandas as pd

load_folder_path = "exports/game_streaks"
save_folder_path = "exports"

df = pd.read_csv(load_folder_path + "/team_game_streaks.csv")
df = df
print(df)

df.groupby("A").agg(
    b_min=pd.NamedAgg(column="B", aggfunc="min"),
    c_sum=pd.NamedAgg(column="C", aggfunc="sum")
)