from pyspark.sql import DataFrame, SparkSession

class Repository:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def save_parquet(self, file_name: str, df: DataFrame):
        df.write.mode('overwrite').parquet(f'{file_name}.parquet')
    
    def load_parquet(self, file_name):
        return self.spark.read.parquet(f'{file_name}.parquet')
