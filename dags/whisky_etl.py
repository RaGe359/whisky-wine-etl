import os
import shutil
import logging
import glob
import time
import pandas as pd
import numpy as np
import pandera as pa
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from pandera import Column, Check, DataFrameSchema
from pandera.errors import SchemaErrors
from sqlalchemy import create_engine, Integer, Float, SmallInteger, Text, VARCHAR
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

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
    dag_id="whisky_etl",
    description="ETL pipeline to scrape and process whisky data from https://whiskyauctioneer.com/",
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["whisky", "etl", "auctioneer"],
)

def whisky_etl_dag():
    
    @task
    def extract_data():
        logger.info("Starting scraping of website https://whiskyauctioneer.com/")

        chrome_options = Options()
        chrome_options.add_argument("--headless") 
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")

        driver = webdriver.Chrome(options=chrome_options)
        auction_url = "https://whiskyauctioneer.com/whisky-auctions"
        lots_url = "https://whiskyauctioneer.com/whisky-auctions/may-2025-auction/lots"

        

    #########################################
    extract = extract_data()

    extract

dag = whisky_etl_dag()
