import os
from kaggle.api.kaggle_api_extended import KaggleApi

os.environ['KAGGLE_CONFIG_DIR'] = "D:\Games\wproject\whisky-wine-airflow\whisky-wine-etl\dags"
dataset = 'zynicide/wine-reviews'  
download_path = os.path.join(os.getcwd(), 'dags', 'testov')
os.makedirs(download_path, exist_ok=True)

api = KaggleApi()
api.authenticate()
api.dataset_download_files(dataset, path=download_path, unzip=True)