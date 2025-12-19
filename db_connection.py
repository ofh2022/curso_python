import os
import pyodbc
from dotenv import load_dotenv
from sqlalchemy import create_engine


load_dotenv()

def get_sqlserver_connection():
    driver = os.getenv("MSSQL_DRIVER")
    server = os.getenv("MSSQL_HOST")
    port = os.getenv("MSSQL_PORT", "1433")
    database = os.getenv("MSSQL_DB")
    user = os.getenv("MSSQL_USER")
    pwd = os.getenv("MSSQL_PASS")

    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={server},{port};"
        f"DATABASE={database};"
        f"UID={user};"
        f"PWD={pwd};"
        f"TrustServerCertificate=yes;"
    )
    return pyodbc.connect(conn_str)


def get_postgres_connection():
    pg_host = os.getenv("PG_HOST")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DB")
    pg_user = os.getenv("PG_USER")
    pg_pass = os.getenv("PG_PASS")

    conn_str = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    print("Connecting to Postgres:", pg_host, pg_db)

    engine = create_engine(conn_str)

    return engine.connect()