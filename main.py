import kagglehub
import pandas as pd

directory_path = kagglehub.dataset_download("asaniczka/top-spotify-songs-in-73-countries-daily-updated")
csv_path = directory_path + '/' + 'universal_top_spotify_songs.csv'

df = pd.read_csv(csv_path)

print(len(df))