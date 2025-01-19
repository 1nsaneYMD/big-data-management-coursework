class UtilsService:
    @classmethod
    def _get_top_n(cls, data, column_name, top_n):
        top_n = data[column_name].value_counts() \
                    .head(top_n) \
                    .reset_index()
        return top_n
    
    @classmethod
    def get_top_artists(cls, data, column_name, top_n=10):
        top_artists = cls._get_top_n(data, column_name, top_n)
        top_artists.columns = ['artist', 'appearance_count']
        return top_artists
    
    @classmethod
    def get_top_songs_by_appearance(cls, data, top_n=10):
        top_songs = cls._get_top_n(data, 'name', top_n)
        top_songs.columns = ['song', 'appearance_count']
        return top_songs
    
    @classmethod
    def get_top_albums_by_appearance(cls, data, top_n=10):
        top_albums = cls._get_top_n(data, 'album_name', top_n)
        top_albums.columns = ['album', 'appearance_count']
        top_albums['album_with_artist'] = top_albums['album'] + " - " + top_albums['album'].apply(lambda album: data[data['album_name'] == album]['main_artist'].mode()[0])
        return top_albums
    
    @classmethod
    def get_top_rank_1_songs(cls, data, top_n=10):
        rank_1_songs = data[data['daily_rank'] == 1]
        top_rank_1_songs = cls._get_top_n(rank_1_songs, 'name', top_n)
        top_rank_1_songs.columns = ['song', 'number_of_rank_1']
        top_rank_1_songs['song_with_artist'] = top_rank_1_songs['song'] + " - " + top_rank_1_songs['song'].apply(lambda album: data[data['name'] == album]['main_artist'].mode()[0])
        return top_rank_1_songs
    
    @classmethod
    def get_artist_with_most_unique_rank_1_songs(cls, data, top_n=10):
        rank_1_songs = data[data['daily_rank'] == 1]
        artist_unique_rank_1 = rank_1_songs.groupby('main_artist')['name'] \
            .nunique() \
            .sort_values(ascending=False) \
            .head(top_n) \
            .reset_index()
        artist_unique_rank_1.columns = ['artist', 'unique_rank_1_songs']
        return artist_unique_rank_1

