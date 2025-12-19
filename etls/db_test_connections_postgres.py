#testando conexão com o postgres

import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

def connect_postgres():
    pg_host = os.getenv("PG_HOST")
    pg_port = os.getenv("PG_PORT", "5432")
    pg_db = os.getenv("PG_DB")
    pg_user = os.getenv("PG_USER")
    pg_pass = os.getenv("PG_PASS")

    conn_str = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
    print("Connecting to Postgres:", pg_host, pg_db)

    engine = create_engine(conn_str, connect_args={"connect_timeout": 10})

    with engine.connect() as conn:
        version = conn.execute(text("SELECT version()")).fetchone()
        print("Postgres version:", version[0])

        # # sanity check (opcional)
        # try:
        #     res = conn.execute(text("SELECT COUNT(*) FROM vendas")).fetchone()
        #     print("Count vendas:", res[0])
        # except Exception as e:
        #     print("Tabela 'vendas' não encontrada (ok):", str(e))

    engine.dispose()


if __name__ == "__main__":
    connect_postgres()
