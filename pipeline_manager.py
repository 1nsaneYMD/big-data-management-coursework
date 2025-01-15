from repository import Repository
from spark_session_client import SparkSessionClient
from task_manager import TaskManager
from prefect import task, flow


class PipelineManager:
    def __init__(self):
        self.repository = Repository()
        self.spark_session = SparkSessionClient().get_session()
        self.task_manager = TaskManager(self.repository, self.spark_session)

pipeline_manager = PipelineManager()

@task(retries=2, retry_delay_seconds=5)
def task_load_data():
    pipeline_manager.task_manager.load_data()
    return 'Data loaded successfully'

@task(retries=2, retry_delay_seconds=5)
def task_analyze_data():
    pipeline_manager.task_manager.analyze_data()
    return 'Data analyzed successfully'

@flow(name='Load Data Flow')
def run_load_data_flow():
    task_analyze_data()
