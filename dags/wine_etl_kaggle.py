import os
import shutil
import logging
import glob
import pandas as pd
import numpy as np
import pandera as pa
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from kaggle.api.kaggle_api_extended import KaggleApi
from pandera import Column, Check, DataFrameSchema
from pandera.errors import SchemaErrors
from sqlalchemy import create_engine, Integer, Float, SmallInteger, Text, VARCHAR
# from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

default_args = {
    "owner": "airflow", 
    # "email": [""],   
    # "email_on_failure": False,
    # "email_on_retry": False,
    "retries": None,    
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=30), 
}

@dag(
    dag_id="wine_etl_kaggle",
    description="ETL pipeline to download and process wine data from Kaggle",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["wine", "etl", "kaggle"],
)

def wine_etl_dag():
    data_path = "/opt/airflow/data"

    @task()
    def extract_data() -> str:
        logger.info("Starting data download from Kaggle API")

        os.environ['KAGGLE_CONFIG_DIR'] = "/home/airflow/.kaggle"
        dataset = 'zynicide/wine-reviews'  
        os.makedirs(data_path, exist_ok=True)

        api = KaggleApi()
        api.authenticate()
        api.dataset_download_files(dataset, path=data_path, unzip=True)
        json_files = glob.glob(os.path.join(data_path, "*.json"))

        if not json_files:
            logger.error("No JSON file found after download.")
            raise FileNotFoundError("No JSON file found after download.")
        
        logger.info(f"Downloaded and extracted dataset: {json_files[0]}")
        return json_files[0]

    @task()
    def process_data(json_file_path: str) -> str:
        logger.info(f"Reading JSON data from: {json_file_path}")
        df = pd.read_json(json_file_path)
        
        logger.info("Cleaning and transforming data...")

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

        transformed_data_path = json_file_path.replace(".json", "_transformed.csv")
        df.to_csv(transformed_data_path, index=False)
        logger.info(f"Saved transformed data to: {transformed_data_path}")

        return transformed_data_path

    @task()
    def validate_data(transformed_data_path: str) -> str:
        # Data will fail validations, but we will load into DB anyway
        logger.info(f"Validating data at: {transformed_data_path}")
        df = pd.read_csv(transformed_data_path)

        schema = DataFrameSchema(
            {
                "points": Column(
                    pa.Int,
                    checks=Check.in_range(50, 100),
                    nullable=False,
                    description="Wine review score, must be between 50 and 100."
                ),
                "title": Column(
                    pa.String,
                    checks=Check.str_length(3, 200),
                    nullable=True,
                    description="Review title"
                ),
                "description": Column(
                    pa.String,
                    checks=Check.str_length(min_value=10),
                    nullable=True
                ),
                "taster_name": Column(
                    pa.String, 
                    nullable=True),
                "taster_twitter_handle": Column(
                    pa.String,
                    nullable=True
                ),
                "price": Column(
                    pa.Float,
                    checks=Check.ge(0),
                    nullable=True,
                    description="Wine price in USD must be positive."
                ),
                "designation": Column(pa.String, nullable=True),
                "variety": Column(pa.String, nullable=True),
                "region_1": Column(pa.String, nullable=True),
                "region_2": Column(pa.String, nullable=True),
                "province": Column(pa.String, nullable=True),
                "country": Column(
                    pa.String,
                    checks=Check.isin(["US", "France", "Italy", "Spain", "Argentina", "Chile", "Australia", "Germany"]),
                    nullable=False
                ),
                "winery": Column(pa.String, nullable=True),
                "title_length": Column(pa.Int, checks=Check.ge(0), nullable=False),
                "description_length": Column(pa.Int, checks=Check.ge(0), nullable=False),
                "price_category": Column(pa.String, nullable=False),
                "region": Column(pa.String, nullable=False),
                "country_code": Column(pa.Int, nullable=False),
            },
            strict=False,
            coerce=True,
        )

        try:
            schema.validate(df, lazy=True)
            logger.info("Validation passed with no issues.")
        except SchemaErrors as e:
            logger.warning("Validation completed with issues:")
            logger.warning(f"{e.failure_cases[['column', 'failure_case', 'index']]}")
            return transformed_data_path
        
        return transformed_data_path

    @task()
    def save_to_database(transformed_data_path: str):
        logger.info(f"Loading data into database from: {transformed_data_path}")
        df = pd.read_csv(transformed_data_path)

        db_url = os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')
        engine = create_engine(db_url)
        
        dtype_mapping = {
            "points": Integer,
            "title": VARCHAR(length=255),
            "description": Text,
            "taster_name": VARCHAR(length=255),
            "taster_twitter_handle": VARCHAR(length=255),
            "price": Float,
            "designation": VARCHAR(length=255),
            "variety": VARCHAR(length=255),
            "region_1": VARCHAR(length=255),
            "region_2": VARCHAR(length=255),
            "province": VARCHAR(length=255),
            "country": VARCHAR(length=255),
            "winery": VARCHAR(length=255),
            "title_length": Integer,
            "description_length": Integer,
            "price_category": VARCHAR(length=50),
            "region": VARCHAR(length=255),
            "country_code": SmallInteger,
        }
    
        df.to_sql(
            name='wine_data',
            con=engine,
            if_exists='append', 
            index=False,
            dtype=dtype_mapping
        )

        logger.info("Data successfully written to database.")
    
    @task()
    def cleanup_files():
        if os.path.exists(data_path) and os.path.isdir(data_path):
            logger.info(f"Cleaning up files in: {data_path}")
            for filename in os.listdir(data_path):
                file_path = os.path.join(data_path, filename)
                
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path) 
                        logger.info(f"Deleted file: {file_path}")
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                        logger.info(f"Deleted directory: {file_path}")
                except Exception as e:
                    logger.error(f"Failed to delete {file_path}. Reason: {e}")
        else:
            logger.warning(f"Directory {data_path} does not exist")


    with TaskGroup('extract') as extract_group:
        extract = extract_data()
    with TaskGroup('transform') as transform_group:
        transform = process_data(extract)
        validate = validate_data(transform)
        transform >> validate
    with TaskGroup('load') as load_group:
        load = save_to_database(validate)
        cleanup = cleanup_files()
        load >> cleanup

    trigger_whisky_dag = TriggerDagRunOperator(
    task_id="trigger_whisky_etl",
    trigger_dag_id="whisky_etl",
    wait_for_completion=False,
    deferrable=True,
    )

    extract_group >> transform_group >> load_group >> trigger_whisky_dag

dag = wine_etl_dag()
