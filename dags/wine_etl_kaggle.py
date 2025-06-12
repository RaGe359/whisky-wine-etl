import os
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from kaggle.api.kaggle_api_extended import KaggleApi


default_args = {
    "owner": "airflow", 
    # "email": [""],   
    # "email_on_failure": False,
    # "email_on_retry": False,
    "retries": 2,    
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30), 
}

@dag(
    dag_id="wine_kaggle_etl",
    description="ETL pipeline to download and process wine data from Kaggle",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["wine", "etl", "kaggle"],
)

def wine_etl_dag():   
    @task()
    def download_data():
        os.environ['KAGGLE_CONFIG_DIR'] = "/home/airflow/.kaggle"
        dataset = 'zynicide/wine-reviews'  
        download_path = os.path.join(os.getcwd(), 'data', 'wine')
        os.makedirs(download_path, exist_ok=True)

        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(dataset, path=download_path, unzip=True)

    @task()
    def process_data():
        print("Processing wine data...")
        # Placeholder for data processing logic

    @task()
    def save_to_database():
        print("Saving processed data to database...")
        # Placeholder for saving logic

    extract = download_data()
    transform = process_data()
    load = save_to_database()

    extract >> transform >> load

dag = wine_etl_dag()
