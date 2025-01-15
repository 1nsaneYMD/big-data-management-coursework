from pyspark.sql import SparkSession


class SparkSessionClient:
    def __init__(self):
        self.instance = None

    def get_session(self) -> SparkSession:
        if self.instance is None:
            self.instance = SparkSession.builder \
                .appName('Spotify Analysis') \
                .config('spark.log.level', 'ERROR') \
                .getOrCreate()
            
        return self.instance
