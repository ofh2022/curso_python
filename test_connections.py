from db_connection import get_postgres_connection, get_sqlserver_connection
from sqlalchemy import  text


def test_postgres():
    try:
        conn = get_postgres_connection()
        result = conn.execute(text("SELECT version();")).fetchone()
        print("Postgres OK:", result[0])
        conn.close()
    except Exception as e:
        print("ERRO Postgres:", e)


def test_sqlserver():
    try:
        conn = get_sqlserver_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT @@VERSION;")
        row = cursor.fetchone()
        print("SQL Server OK:", row)
        conn.close()
    except Exception as e:
        print("ERRO SQL Server:", e)

if __name__ == "__main__":
    print("### Testando Postgres ###")
    test_postgres()
    print("\n### Testando SQL Server via ODBC ###")
    test_sqlserver()
