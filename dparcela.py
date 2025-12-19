from db_connection import get_postgres_connection, get_sqlserver_connection
import pandas as pd

def etl_parcela():
    """
    ETL de  parcela
    Origem: PostgreSQL (vendas.parcela)
    Destino: SQL Server (dw.parcela)
    """

    # =====================
    # 1. EXTRACT
    # =====================
    query = """
        SELECT
            id,
            id_nota_fiscal,
            numero,
            vencimento,
            valor
        FROM vendas.parcela;
    """

    pg_conn = get_postgres_connection()
    df = pd.read_sql(query, pg_conn)
    pg_conn.close()

    return df
df = etl_parcela()

# =====================
    # 2. TRANSFORM
    # =====================
df = df.copy()

# df["id_produto"] = df["id_produto"].astype("int64")
# df["id_nota_fiscal"] = df["id_nota_fiscal"].astype("int64")
# df["quantidade"] = df["quantidade"].astype("int64")

# Remover duplicidades pela PK de origem
df = df.drop_duplicates(subset=["id"])

# Regra mínima de qualidade
#df = df[df["quantidade"] > 0]

# =====================
# 3. LOAD
# =====================
mssql_conn = get_sqlserver_connection()
cursor = mssql_conn.cursor()

insert_sql = """
    INSERT INTO dbo.parcela (
    id_parcela, id_nota_fiscal, numero, vencimento, valor
    )
    VALUES (?, ?, ?, ?, ?)
"""

for _, row in df.iterrows():
    cursor.execute(insert_sql, (
    row["id"],
    row["id_nota_fiscal"],
    row["numero"],
    row["vencimento"],
    row["valor"]
    ))

mssql_conn.commit()
cursor.close()
mssql_conn.close()

print("✔ Dados carregados no SQL Server")
print("🏁 ETL finalizado com sucesso")