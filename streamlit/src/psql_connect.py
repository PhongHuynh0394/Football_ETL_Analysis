import os
import psycopg2
from dotenv import load_dotenv
import pandas as pd

#load environment
load_dotenv()

#list table in database
table = ['statsperleagueseason','statsperplayerseason', 'statsplayerper90']

PSQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD")
}

#create connection
def init_connection(config):
    return psycopg2.connect(
        database=config['database'],
        user=config['user'],
        password=config['password'],
        host=config['host'],
        port=config['port']
    )

def extract_data():
    conn = init_connection(PSQL_CONFIG)
    return [pd.read_sql(f'SELECT * FROM analysis.{tab}', conn) for tab in table]