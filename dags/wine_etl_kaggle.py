import os, logging, glob, pandas as pd, numpy as np, pandera.pandas as pa
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from kaggle.api.kaggle_api_extended import KaggleApi
from airflow.models import Variable
from pandera import Column, Check, DataFrameSchema


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
        print("Placeholder")
        os.environ['KAGGLE_CONFIG_DIR'] = "/home/airflow/.kaggle"
        dataset = 'zynicide/wine-reviews'  
        data_path = "/opt/airflow/data"
        os.makedirs(data_path, exist_ok=True)

        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(dataset, path=data_path, unzip=True)

        json_files = glob.glob(os.path.join(data_path, "*.json"))
        if not json_files:
            raise FileNotFoundError("No JSON file found after download.")
        
        return json_files[0]

    @task()
    def process_data(json_file_path):
        df = pd.read_json(json_file_path)

        df['points'] = pd.to_numeric(df['points'], errors='coerce')
        df = df.dropna(subset=['points'])
        df['taster_twitter_handle'] = df['taster_twitter_handle'].fillna('unknown')
        df['taster_twitter_handle'] = df['taster_twitter_handle'].str.replace('@', '', regex=False)
        df['price'] = df['price'].fillna(df['price'].median())
        df['designation'] = df['designation'].fillna('unknown')
        df['winery'] = df['winery'].fillna('unknown')

        df['title_length'] = df['title'].apply(lambda x: len(str(x)) if pd.notnull(x) else 0)
        df['description_length'] = df['description'].apply(lambda x: len(str(x)) if pd.notnull(x) else 0)

        bins = [0, 20, 50, 100, 500, np.inf]
        labels = ['cheap', 'affordable', 'midrange', 'premium', 'luxury']
        df['price_category'] = pd.cut(df['price'], bins=bins, labels=labels)

        df['region'] = df['region_1'].combine_first(df['region_2'])
        df['region'] = df['region'].fillna('unknown')
        df['country_code'] = df['country'].astype('category').cat.codes

        transformed_data = json_file_path.replace(".json", "_transformed.csv")
        df.to_csv(transformed_data, index=False)

        return transformed_data

    @task()
    def validate_data(transformed_data):
        print("WIP")

    @task()
    def save_to_database():
        print("Saving processed data to database...")
        # Placeholder for saving logic

    extract = download_data()
    transform = process_data(extract)
    validate = validate_data(transform)
    load = save_to_database(transform)

    extract >> transform >> validate >> load

dag = wine_etl_dag()
