from services.etl_service import ETLService
from services.utils import UtilsService
from services.plot_utils import PlotUtilsService
from services.preprocessor import preprocess_data

class AnalysisService:
    def __init__(self):
        self.etl_service = ETLService()
    
    def preprocess_data(self, input_path='data/big_data.csv'):
        df = self.etl_service.load_csv_into_dataframe(input_path)
        tracks, artists = preprocess_data(df)

        self.etl_service.save_dataframe_to_csv(tracks, 'data/tracks_cleaned.csv')
        self.etl_service.save_dataframe_to_csv(artists, 'data/artists_cleaned.csv')

    def _generate_bar_plot(self, data, x, y, title, xlabel, ylabel, filename):
        fig = PlotUtilsService.plot_bar_plot(data, x, y, title, xlabel, ylabel)
        fig.savefig(filename)
        PlotUtilsService.close_figure(fig)

    def _generate_line_plot(self, data, filename):
        fig = PlotUtilsService.plot_line_plot(data)
        fig.savefig(filename)
        PlotUtilsService.close_figure(fig)

    def get_top_artists(self):
        artists_df = self.etl_service.load_csv_into_dataframe('data/artists_cleaned.csv')
        tracks_df = self.etl_service.load_csv_into_dataframe('data/tracks_cleaned.csv')

        top_artists_data = [
            (UtilsService.get_top_artists(artists_df, 'artist'), 'Top 10 Artists by Appearance with features', 'top_artists_with_features.png'), 
            (UtilsService.get_top_artists(tracks_df, 'main_artist'), 'Top 10 Artists by Appearance without features', 'top_artists_without_features.png')
        ]

        for data, title, filename in top_artists_data:
            self._generate_bar_plot(data, 'artist', 'appearance_count', title, 'Artist', 'Number of Appearances', filename)

        return 'Top artists bar plot generated successfully'
    
    def get_top_albums_by_appearance(self):
        tracks_df = self.etl_service.load_csv_into_dataframe('data/tracks_cleaned.csv')
        top_albums = UtilsService.get_top_albums_by_appearance(tracks_df)
        self._generate_bar_plot(top_albums, 'album_with_artist', 'appearance_count',
                                'Top 10 Albums by Appearance in Dataset', 'Album with Artist',
                                'Number of Appearances', 'top_albums_appearance.png')
        return 'Top albumns by appearance bar plot generated successfully'

    def get_top_songs_by_appearance(self):
        tracks_df = self.etl_service.load_csv_into_dataframe('data/tracks_cleaned.csv')
        top_songs = UtilsService.get_top_songs_by_appearance(tracks_df)
        self._generate_bar_plot(top_songs, 'song', 'appearance_count',
                                'Top 10 Songs by Appearance in Dataset', 'Song',
                                'Number of Appearances', 'top_songs_appearance.png')
        return 'Top songs by appearance bar plot generated successfully'
    
    def get_top_rank_1_songs(self):
        tracks_df = self.etl_service.load_csv_into_dataframe('data/tracks_cleaned.csv')
        top_rank_1_songs = UtilsService.get_top_rank_1_songs(tracks_df)
        self._generate_bar_plot(top_rank_1_songs, 'song_with_artist', 'number_of_rank_1', 
                           'Songs with the Most #1 Rankings', 'Song', 
                           'Number of Times Ranked #1', 'top_rank_1_songs.png')
        return 'Top rank 1 songs bar plot generated successfully'
    
    def get_artist_with_most_unique_rank_1_songs(self):
        tracks_df = self.etl_service.load_csv_into_dataframe('data/tracks_cleaned.csv')
        top_artists_rank_1 = UtilsService.get_artist_with_most_unique_rank_1_songs(tracks_df)
        self._generate_bar_plot(top_artists_rank_1, 'artist', 'unique_rank_1_songs',
                                'Artists with Most Unique #1 Songs', 'Artist', 
                                'Number of Unique #1 Songs', 'artist_with_most_unique_rank_1_songs.png')
        return 'Artist with most unique rank 1 songs bar plot generated successfully'
    
    def get_global_rank_1_songs_details(self):
        tracks_df = self.etl_service.load_csv_into_dataframe('data/tracks_cleaned.csv')
        self._generate_line_plot(tracks_df, 'global_rank_1_songs_details.png')
        return 'Global rank 1 songs details line plot generated successfully'

# rm -rf ~/airflow/dags
# airflow standalone
# cp -r ./reworked_version ~/airflow/dags
