from services.etl_service import ETLService
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, max, min

class AnalysisService:
    def __init__(self):
        self.etl_service = ETLService()

    def load_data(self):
        return self.etl_service.load_csv_to_parquet()
    
    def top_tracks(self, input_path='data/spotify_tracks.parquet', limit=3):
        df = self.etl_service.load_parquet(input_path)
        # df.orderBy(col('popularity').desc()).select('name', 'artists', 'popularity').limit(limit)
        return 'Top tracks generated successfully'
    
    def average_metrics(self, df: DataFrame):
        return df.select(
            avg('danceability').alias('avg_danceability'),
            avg('energy').alias('avg_energy'),
            avg('tempo').alias('avg_tempo')
        )
    
    def popularity_extremes(self, df: DataFrame):
        return df.select(
            max('popularity').alias('max_popularity'),
            min('popularity').alias('min_popularity')
        )
    
if __name__ == '__main__':
    analysis_service = AnalysisService()
    df = analysis_service.load_data()
    top_songs = analysis_service.top_tracks(df).collect()
    average_metrics = analysis_service.average_metrics(df).collect()

    print('Top songs:', top_songs)
    print('Average metrics:', average_metrics)
