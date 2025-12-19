#testando conexão com o sql server

import os
import urllib
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def connect_sqlserver():
    m_host = os.getenv("MSSQL_HOST")
    m_port = os.getenv("MSSQL_PORT", "1433")
    m_db = os.getenv("MSSQL_DB")
    m_user = os.getenv("MSSQL_USER")
    m_pass = os.getenv("MSSQL_PASS")
    m_driver = os.getenv("MSSQL_DRIVER", "{ODBC Driver 18 for SQL Server}")

    params = (
        f"DRIVER={m_driver};"
        f"SERVER={m_host},{m_port};"
        f"DATABASE={m_db};"
        f"UID={m_user};PWD={m_pass};"
        "TrustServerCertificate=yes;"
    )

    odbc_str = urllib.parse.quote_plus(params)
    conn_str = f"mssql+pyodbc:///?odbc_connect={odbc_str}"

    print("Connecting to SQL Server:", m_host, m_db)

    engine = create_engine(conn_str, connect_args={"timeout": 10})

    with engine.connect() as conn:
        try:
            version = conn.execute(text("SELECT @@VERSION")).fetchone()
            print("SQL Server version:", version[0][:200])
        except Exception as e:
            print("Erro ao obter versão do SQL Server:", str(e))

        try:
            res = conn.execute(text("SELECT DB_NAME()")).fetchone()
            print("Current DB:", res[0])
        except Exception as e:
            print("Erro no sanity check:", str(e))

    engine.dispose()


if __name__ == "__main__":
    connect_sqlserver()
