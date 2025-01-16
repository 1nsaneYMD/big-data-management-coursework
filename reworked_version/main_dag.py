from datetime import datetime
from airflow import DAG
from services.airflow_operator import AirflowOperator

with DAG(
    dag_id='spotify_analysis_pipeline',
    description='A pipeline to analyze Spotify track data',
    start_date=datetime(2025, 1, 1),
    catchup=False
) as dag:
    data_pipeline = AirflowOperator(dag)
    data_pipeline.run_pipeline()
