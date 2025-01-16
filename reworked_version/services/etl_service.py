from services.spark_service import SparkService

class ETLService:
    def __init__(self):
        self.spark = SparkService().get_spark()

    def load_csv_to_parquet(self, input_path='data/spotify_tracks.csv', output_path='data/spotify_tracks.parquet'):
        df = self.spark.read.csv(input_path, header=True, inferSchema=True)
        df.write.mode('overwrite').parquet(output_path)

    def load_parquet(self, input_path='data/spotify_tracks.parquet'):
        return self.spark.read.parquet(input_path)

if __name__ == '__main__':
    etl_service = ETLService()
    etl_service.load_csv_to_parquet()

    df = etl_service.load_parquet()
