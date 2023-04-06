from contextlib import contextmanager
from datetime import datetime
import pandas as pd
from dagster import IOManager, OutputContext, InputContext
from sqlalchemy import create_engine

@contextmanager
def connect_mysql(config):
    conn_info = (
        f"mysql+pymysql://{config['user']}:{config['password']}" + f"@{config['host']}:{config['port']}" + f"/{config['database']}"
    )

    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        raise

class MySQLIOManager(IOManager):
    def __init__(self, config): 
       self.config = config

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
       pass

    def load_input(self, context: InputContext) -> pd.DataFrame:
       pass

    def extract_data(self, sql: str) -> pd.DataFrame:
        with connect_mysql(self.config) as db_conn:
            pd_data = pd.read_sql_query(sql, db_conn)
            return pd_data