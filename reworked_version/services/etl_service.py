from services.spark_service import SparkService
from pyspark.sql import DataFrame

class ETLService:
    def __init__(self):
        self.spark = SparkService().get_spark()

    def load_csv_to_parquet(self, input_path='data/spotify_tracks.csv', output_path='data/spotify_tracks.parquet'):
        df = self.spark.read.csv(input_path, header=True, inferSchema=True)
        self.save_as_parquest(df, output_path)

    def load_parquet(self, input_path='data/spotify_tracks.parquet'):
        return self.spark.read.parquet(input_path)
    
    def save_as_parquest(self, df: DataFrame, output_path):
        df.write.mode('overwrite').parquet(output_path)

    def register_udf_function(self, name, f):
        return self.spark.udf.register(name, f)

if __name__ == '__main__':
    etl_service = ETLService()
    etl_service.load_csv_to_parquet()

    df = etl_service.load_parquet()
