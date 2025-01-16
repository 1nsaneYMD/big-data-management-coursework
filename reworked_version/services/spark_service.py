from pyspark.sql import SparkSession

class SparkService:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SparkService, cls).__new__(cls)
            cls._instance.spark = SparkSession.builder \
                .appName('Spotify Data Pipeline') \
                .config('spark.sql.parquet.compression.codec', 'snappy') \
                .getOrCreate()
        return cls._instance
    
    def get_spark(self) -> SparkSession:
        return self._instance.spark
    
    def close(self):
        if self._instance and self._instance.spark:
            self._instance.spark.stop()
            self._instance = None

if __name__ == '__main__':
    spark_service_a = SparkService()
    spark = spark_service_a.get_spark()

    spark_service_b = SparkService()
    spark = spark_service_b.get_spark()
