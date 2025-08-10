# app/queries.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
import ast
from collections import defaultdict, Counter

# Use environment variables for Docker compatibility
db_user = os.getenv('POSTGRES_USER', 'admin')
db_password = os.getenv('POSTGRES_PASSWORD', 'admin123')
db_host = os.getenv('POSTGRES_HOST', 'db')  # Docker Compose service name
db_port = os.getenv('POSTGRES_PORT', '5432')
db_name = os.getenv('POSTGRES_DB', 'mydb')

engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# KDA and Winrate

def get_kda_winrate():
    query = """
    SELECT 
        puuid,
        SUM(kills) AS total_kills,
        SUM(assist_count) AS total_assists,
        SUM(death_count) AS total_deaths,
        ROUND((SUM(kills) + SUM(assist_count))::numeric / NULLIF(SUM(death_count), 0), 2) AS kda_ratio,
        SUM(CASE WHEN player_won_round = TRUE THEN 1 ELSE 0 END) AS total_rounds_won,
        COUNT(*) AS total_rounds_played,
        CONCAT(ROUND(SUM(CASE WHEN player_won_round = TRUE THEN 1 ELSE 0 END)::numeric / COUNT(*) * 100, 0), '%') AS win_rate
    FROM round_data
    GROUP BY puuid
    ORDER BY kda_ratio DESC;
    """
    with engine.connect() as conn:
        df = pd.read_sql_query(text(query), conn)
    return df

# Top 3 Victims

def get_top_3_victims():
    query = "SELECT puuid, kill_victim_puuids FROM round_data;"
    with engine.connect() as conn:
        df = pd.read_sql_query(text(query), conn)

    kill_counts = defaultdict(Counter)

    for _, row in df.iterrows():
        killer = row['puuid']
        victim_str = row['kill_victim_puuids']

        if pd.isna(victim_str) or victim_str.strip() == '':
            continue

        try:
            victims = ast.literal_eval(victim_str)
            for victim in victims:
                kill_counts[killer][victim] += 1
        except Exception as e:
            print(f"Error parsing: {victim_str}, error: {e}")

    results = []
    for killer, counter in kill_counts.items():
        top3 = counter.most_common(3)
        top_victims = [v for v, _ in top3]
        top_victims += ['N/A'] * (3 - len(top_victims))
        results.append({
            'puuid': killer,
            'top1_victim': top_victims[0],
            'top2_victim': top_victims[1],
            'top3_victim': top_victims[2]
        })

    return pd.DataFrame(results)

# Damage Stats

def get_damage_stats():
    query = "SELECT puuid, damage_done FROM round_data;"
    with engine.connect() as conn:
        df = pd.read_sql_query(text(query), conn)

    damage_counts = defaultdict(lambda: {'head': 0, 'body': 0, 'leg': 0})

    for _, row in df.iterrows():
        puuid = row['puuid']
        damage_str = row['damage_done']

        if pd.isna(damage_str) or damage_str.strip() == '':
            continue

        try:
            dmg_data = ast.literal_eval(damage_str)
            head = dmg_data.get('headshots', 0)
            body = dmg_data.get('bodyshots', 0)
            leg = dmg_data.get('legshots', 0)
            damage_counts[puuid]['head'] += head
            damage_counts[puuid]['body'] += body
            damage_counts[puuid]['leg'] += leg
        except Exception as e:
            print(f"Error parsing: {damage_str}, error: {e}")

    results = []
    for puuid, counts in damage_counts.items():
        head = counts['head']
        body = counts['body']
        leg = counts['leg']
        total_shots = head + body + leg
        accuracy = round((head / total_shots) * 100, 2) if total_shots > 0 else 0.0
        results.append({
            'puuid': puuid,
            'headshots': head,
            'bodyshots': body,
            'legshots': leg,
            'headshot_accuracy': accuracy
        })

    result_df = pd.DataFrame(results)

    if not result_df.empty:
        result_df['accuracy_percentile'] = result_df['headshot_accuracy'].rank(pct=True).apply(lambda x: f"{round(x*100, 2)}%")
    else:
        result_df['accuracy_percentile'] = []

    return result_df
