from airflow.operators.python import PythonOperator
from .analysis_service import AnalysisService

class AirflowOperator:
    def __init__(self, dag):
        self.dag = dag
        self.analysis_service = AnalysisService()

    def create_operator(self, task_id, python_callable):
        return PythonOperator(
            task_id=task_id,
            python_callable=python_callable,
            dag=self.dag
        )
    
    def load_data(self):
        return self.create_operator('load_data', self.analysis_service.load_data)
    
    def top_tracks_task(self):
        return self.create_operator('analyze_top_tracks', self.analysis_service.top_tracks)
    
    def run_pipeline(self):
        load_task = self.load_data()
        top_tracks_task = self.top_tracks_task()

        load_task >> top_tracks_task
