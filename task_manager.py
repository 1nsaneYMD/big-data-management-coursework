from repository import Repository
from data_loader import DataLoader


class TaskManager:
    def __init__(self, repository: Repository, data_loader: DataLoader):
        self.repository = repository
        self.data_loader = data_loader
    
    def load_and_save_data(self, input_path: str, output_name: str):
        df = self.data_loader.load_csv(input_path)
        self.repository.save_parquet(output_name, df)

    def analyze_data(self, file_name: str):
        df = self.repository.load_parquet(file_name)
        print(f'Row Count: {df.count()}')
