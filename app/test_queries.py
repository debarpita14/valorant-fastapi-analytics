from queries import get_kda_winrate

df = get_kda_winrate()
print(df.head())

from queries import get_top_3_victims

df = get_top_3_victims()
print(df.head())

from queries import get_damage_stats

df = get_damage_stats()
print(df.head())

