from data_loader import DataLoader


class TaskManager:
    def __init__(self, repository, spark_session):
        self.repository = repository
        self.spark_session = spark_session
        self.data_loader = DataLoader(spark_session)
    
    def load_data(self):
        spotify_df = self.data_loader.load_data()
        self.repository.save_as_parquet('spotify', spotify_df)

    def analyze_data(self):
        spotify_df = self.repository.load_parquet('spotify')
        print('COUNT', spotify_df.count())
