from spark_session_client import SparkSessionClient
from pyspark.sql import DataFrame

class Repository:
    def __init__(self):
        self.spark_session = SparkSessionClient().get_session()

    def save_as_parquet(self, file_name, df: DataFrame):
        output_file = '{}.parquet'.format(file_name)
        df.write.mode('overwrite').parquet(output_file)
    
    def load_parquet(self, file_name):
        input_file = '{}.parquet'.format(file_name)
        return self.spark_session.read.parquet(input_file)
