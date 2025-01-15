from pyspark.sql import SparkSession


class SparkSessionClient:
    _instance = None

    @classmethod
    def get_session(cls) -> SparkSession:
        if cls._instance is None:
            cls._instance = SparkSession.builder \
                .appName('Spotify Analysis') \
                .config('spark.log.level', 'ERROR') \
                .getOrCreate()
        return cls._instance
