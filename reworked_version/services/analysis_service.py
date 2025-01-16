from services.etl_service import ETLService
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, max, min

class AnalysisService:
    def __init__(self):
        self.etl_service = ETLService()

    def load_data(self):
        return self.etl_service.load_csv_to_parquet()
    
    def top_tracks(self, input_path='data/spotify_tracks.parquet', limit=10):
        df = self.etl_service.load_parquet(input_path)
        top_tracks_data = df.orderBy(col('popularity').desc()) \
                            .select('name', 'artists', 'popularity') \
                            .limit(limit)
        self.etl_service.save_as_parquest(top_tracks_data, 'data/top_tracks.parquet')
        return 'Top tracks generated successfully'
    
    def average_metrics(self, input_path='data/spotify_tracks.parquet'):
        df = self.etl_service.load_parquet(input_path)
        average_metrics_data = df.select(
            avg('danceability').alias('avg_danceability'),
            avg('energy').alias('avg_energy'),
            avg('tempo').alias('avg_tempo')
        )
        self.etl_service.save_as_parquest(average_metrics_data, 'data/average_metrics.parquet')
        return 'Average metrics for tracks generated successfully'
    
    def popularity_extremes(self, df: DataFrame):
        return df.select(
            max('popularity').alias('max_popularity'),
            min('popularity').alias('min_popularity')
        )
    
if __name__ == '__main__':
    analysis_service = AnalysisService()
    df = analysis_service.load_data()
    average_metrics = analysis_service.average_metrics()

    print('Average metrics:', average_metrics)
