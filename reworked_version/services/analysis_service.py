from services.etl_service import ETLService
from services.utils import get_continent_udf, iso_to_country_name
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, max, min, when, lit, to_date, split, expr

class AnalysisService:
    def __init__(self):
        self.etl_service = ETLService()

    def load_data(self):
        return self.etl_service.load_csv_to_parquet()
    
    def preprocess_data(self, input_path='data/spotify_tracks.parquet'):
        df = self.etl_service.load_parquet(input_path)
        get_continent_udf_spark = self.etl_service.register_udf_function('get_continent_udf', get_continent_udf)
        iso_to_country_udf = self.etl_service.register_udf_function('iso_to_country_udf', iso_to_country_name)

        df = df.withColumn('country', when(col('country').isNull(), lit('Global')).otherwise(col('country'))) \
            .dropna(subset=['name', 'artists']) \
            .withColumn('continent', get_continent_udf_spark(col('country'))) \
            .withColumn('country', iso_to_country_udf(col('country'))) \
            .withColumn('snapshot_date', to_date(col('snapshot_date'))) \
            .withColumn('album_release_date', to_date(col('album_release_date'))) \
            .withColumn('artists_array', split(col('artists'), ', ')) \
            .withColumn('main_artist', expr("artists_array[0]")) \
            .withColumn('feature_1', expr("artists_array[1]")) \
            .withColumn('feature_2', expr("artists_array[2]")) \
            .withColumn('feature_3', expr("artists_array[3]")) \
            .drop('artists_array', 'artists')

        self.etl_service.save_as_parquest(df, 'data/preprocessed_data.parquet')
        return 'Data cleaning completed successfully'

    def top_tracks(self, input_path='data/preprocessed_data.parquet', limit=10):
        df = self.etl_service.load_parquet(input_path)
        top_tracks_data = df.orderBy(col('popularity').desc()) \
                            .select('name', 'main_artist', 'popularity') \
                            .limit(limit)
        self.etl_service.save_as_parquest(top_tracks_data, 'data/top_tracks.parquet')
        return 'Top tracks generated successfully'
    
    def average_metrics(self, input_path='data/preprocessed_data.parquet'):
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
    analysis_service.load_data()
    analysis_service.preprocess_data()
    analysis_service.top_tracks()

    # df = analysis_service.load_data()
    # average_metrics = analysis_service.average_metrics()

    # print('Average metrics:', average_metrics)
