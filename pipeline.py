from prefect import task, flow
from repository import Repository
from data_loader import DataLoader
from task_manager import TaskManager
from spark_session_client import SparkSessionClient


@task(retries=2, retry_delay_seconds=5)
def load_data_task(manager: TaskManager, input_path: str, output_name: str):
    manager.load_and_save_data(input_path, output_name)
    return 'Data loading completed'

@task(retries=2, retry_delay_seconds=5)
def analyze_data_task(manager: TaskManager, file_name: str):
    manager.analyze_data(file_name)
    return 'Data analysis completed'

@flow(name='Spotify Data Pipeline')
def spotify_pipeline(input_path: str, output_name: str):
    spark = SparkSessionClient.get_session()
    repository = Repository(spark)
    data_loader = DataLoader(spark)
    manager = TaskManager(repository, data_loader)

    load_data_task(manager, input_path, output_name)
    analyze_data_task(manager, output_name)
