from db_connection import get_postgres_connection, get_sqlserver_connection
import pandas as pd

#extraindo dados do banco de produção


def extract_situacao(limit_days: int = 7):

    query = f"""
        SELECT id,
        descricao
FROM financeiro.situacao_titulo;
    """

    conn = get_postgres_connection()
    df = pd.read_sql(query, conn)
    conn.close()

    return df


df = extract_situacao()

print(df.dtypes)

# =====================
# 3. LOAD (SQL Server)
# =====================
mssql_conn = get_sqlserver_connection()
cursor = mssql_conn.cursor()

insert_sql = """
    INSERT INTO dbo.situacao (
        id_situacao,descricao
    )
    VALUES (?, ?)
"""

for _, row in df.iterrows():
    cursor.execute(insert_sql, (
    row["id"],
    row["descricao"]
    ))

mssql_conn.commit()
cursor.close()
mssql_conn.close()

print("✔ Dados carregados no SQL Server")
print("🏁 ETL finalizado com sucesso")