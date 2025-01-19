from airflow.operators.python import PythonOperator
from services.analysis_service import AnalysisService

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
    
    def preprocess_data_task(self):
        return self.create_operator('preprocess_data', self.analysis_service.preprocess_data)
    
    def top_artists_task(self):
        return self.create_operator('top_artists', self.analysis_service.get_top_artists)
    
    def top_songs_by_appearance(self):
        return self.create_operator('top_songs_appearance', self.analysis_service.get_top_songs_by_appearance)
    
    def top_albums_by_appearance(self):
        return self.create_operator('top_albums_appearance', self.analysis_service.get_top_albums_by_appearance)
    
    def top_rank_1_songs(self):
        return self.create_operator('top_rank_1_songs', self.analysis_service.get_top_rank_1_songs)
    
    def artist_with_most_unique_rank_1_songs(self):
        return self.create_operator('artist_with_most_unique_rank_1_songs', self.analysis_service.get_artist_with_most_unique_rank_1_songs)
    
    def global_rank_1_songs_details(self):
        return self.create_operator('global_rank_1_songs_details', self.analysis_service.get_global_rank_1_songs_details)

    def run_pipeline(self):
        self.preprocess_data_task() >> [self.top_artists_task(), self.top_songs_by_appearance(), 
                                        self.top_albums_by_appearance(), self.top_rank_1_songs(),
                                        self.artist_with_most_unique_rank_1_songs(), self.global_rank_1_songs_details()]
