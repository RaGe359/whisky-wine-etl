import os, pandas as pd, json, numpy as np
import pandera.pandas as pa
from pandera import Column, Check, DataFrameSchema

os.environ['DISABLE_PANDERA_IMPORT_WARNING']='True'
json_path = 'D:/Games/wproject/whisky-wine-airflow/whisky-wine-etl/data/winemag-data-130k-v2.json'

with open(json_path, 'r') as f:
    json_data = json.load(f)

df = pd.json_normalize(json_data)

df['points'] = pd.to_numeric(df['points'], errors='coerce')
df = df.dropna(subset=['points'])
df['taster_twitter_handle'] = df['taster_twitter_handle'].fillna('unknown')
df['taster_twitter_handle'] = df['taster_twitter_handle'].str.replace('@', '', regex=False)
df['price'] = df['price'].fillna(df['price'].median())
df['designation'] = df['designation'].fillna('unknown')
df['winery'] = df['winery'].fillna('unknown')

df['title_length'] = df['title'].apply(lambda x: len(str(x)) if pd.notnull(x) else 0)
df['description_length'] = df['description'].apply(lambda x: len(str(x)) if pd.notnull(x) else 0)

# Price bins
bins = [0, 20, 50, 100, 500, np.inf]
labels = ['cheap', 'affordable', 'midrange', 'premium', 'luxury']
df['price_category'] = pd.cut(df['price'], bins=bins, labels=labels)

df['region'] = df['region_1'].combine_first(df['region_2'])
df['region'] = df['region'].fillna('unknown')
df['country_code'] = df['country'].astype('category').cat.codes

# df.to_csv('test_file.csv', index=False)

# Validation

schema = DataFrameSchema({
    "points": Column(pa.Int, checks=Check.in_range(50, 100), nullable=False),
    "title": Column(pa.String, nullable=True),
    "description": Column(pa.String, nullable=True),
    "taster_name": Column(pa.String, nullable=True),
    "taster_twitter_handle": Column(pa.String, nullable=True),
    "price": Column(pa.Float, nullable=True),
    "designation": Column(pa.String, nullable=True),
    "variety": Column(pa.String, nullable=True),
    "region_1": Column(pa.String, nullable=True),
    "region_2": Column(pa.String, nullable=True),
    "province": Column(pa.String, nullable=True),
    "country": Column(pa.String, nullable=False),
    "winery": Column(pa.String, nullable=True),
})

# Validate the DataFrame against the schema
try:
    validated_df = schema.validate(df, lazy=True)
except pa.errors.SchemaErrors as err:
    print("Validation failed with multiple errors")
    error_df = err.failure_cases


