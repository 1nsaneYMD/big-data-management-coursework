from pyspark.sql import SparkSession


class DataLoader:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def load_csv(self, file_path: str):
        return self.spark.read.csv(file_path, header=True)
