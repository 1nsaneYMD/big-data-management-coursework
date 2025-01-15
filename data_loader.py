from pyspark.sql import SparkSession


class DataLoader:
    def __init__(self, spark_session: SparkSession):
        self.spark_session = spark_session

    def load_data(self):
        songs_df = self.spark_session.read.csv('test_data.csv', header=True)

        # TODO: Filter and clean data
        return songs_df
