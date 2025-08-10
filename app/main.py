from fastapi import FastAPI
from queries import get_kda_winrate, get_top_3_victims, get_damage_stats
from fastapi.responses import JSONResponse
app = FastAPI()

@app.get("/")
def read_root():
    return {"message": "Valorant Round Data API"}

@app.get("/kda-winrate")
def kda_winrate():
    df = get_kda_winrate()
    print(df.columns)
    data = df[['puuid', 'total_kills', 'total_assists', 'total_deaths', 
 'kda_ratio', 'total_rounds_won', 'total_rounds_played', 'win_rate']].to_dict(orient="records")
    return df[["puuid","total_kills"]].to_dict(orient="records")

@app.get("/top-victims")
def top_victims():
    df = get_top_3_victims()
    return df.to_dict(orient="records")

@app.get("/damage-stats")
def damage_stats():
    df = get_damage_stats()
    return df.to_dict(orient="records")