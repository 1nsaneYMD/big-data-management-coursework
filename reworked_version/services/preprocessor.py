import pandas as pd
import pycountry as pc
import pycountry_convert as pcc

def get_continent(code):
    if code == 'Global':
        return 'Global'
    try:
        continent_code = pcc.country_alpha2_to_continent_code(code)
        continent_name_map = {
            'AF': 'Africa',
            'AS': 'Asia',
            'EU': 'Europe',
            'NA': 'North America',
            'SA': 'South America',
            'OC': 'Oceania',
            'AN': 'Antarctica'
        }
        return continent_name_map[continent_code]
    except Exception:
        print(f'Invalid code {code}')

def iso_to_country_name(code):
    if code == 'Global':
        return 'Global'
    try:
        return pc.countries.get(alpha_2=code).name
    except AttributeError:
        print(f'Invalid code {code}')

# TODO: Extract into class with functions
def preprocess_data(tracks):
    tracks['country'] = tracks['country'].fillna('Global')
    tracks.dropna(subset=['name', 'artists'], inplace=True)

    tracks = tracks.drop(columns='spotify_id')
    tracks['continent'] = tracks['country'].apply(get_continent)
    tracks['country'] = tracks['country'].apply(iso_to_country_name)

    artists = tracks['artists'].str.split(', ', expand=True)
    artists.drop(artists.iloc[:, 4:26 ], axis=1, inplace=True)

    artists_column_map = {
        0: 'main_artist',
        1: 'feature_1',
        2: 'feature_2',
        3: 'feature_3'
    }

    artists.rename(columns=artists_column_map, inplace=True)
    tracks = pd.concat([artists, tracks], axis=1)
    tracks.drop(['artists'], axis=1, inplace=True)

    tracks['snapshot_date'] = pd.to_datetime(tracks['snapshot_date'])
    tracks['album_release_date'] = pd.to_datetime(tracks['album_release_date'])

    artist_data = tracks.melt(
        id_vars=['name', 'popularity', 'danceability', 'energy', 'loudness'], 
        value_vars=['main_artist', 'feature_1', 'feature_2', 'feature_3'], 
        value_name='artist'
    )
    artist_data = artist_data.dropna(subset=['artist'])

    return tracks, artist_data

if __name__ == '__main__':
    preprocess_data()
