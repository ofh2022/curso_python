from db_connection import get_postgres_connection, get_sqlserver_connection
import pandas as pd

#extraindo dados do banco de produção


def extract_receber(limit_days: int = 7):

    query = f"""SELECT
        id, id_parcela, vencimento, valor_original, 
        valor_atual, id_situacao, criado_em, atualizado_em, 
        data_recebimento, id_forma_pagamento
FROM financeiro.conta_receber;
    """

    conn = get_postgres_connection()
    df = pd.read_sql(query, conn)
    conn.close()

    return df

df = extract_receber()

print(df.dtypes)

#transformando dados

df = df.copy()

# Garantir tipos de data
#df["emissao"] = pd.to_datetime(df["emissao"])
df["vencimento"] = pd.to_datetime(df["vencimento"])
df["data_recebimento"] = pd.to_datetime(df["data_recebimento"])

# Remover duplicatas por PK
df = df.drop_duplicates(subset=["id"])


# =====================
# 3. LOAD (SQL Server)
# =====================
mssql_conn = get_sqlserver_connection()
cursor = mssql_conn.cursor()

insert_sql = """
    INSERT INTO dbo.conta_receber (
        id, id_parcela, vencimento, valor_original, valor_atual, 
        id_situacao, criado_em, atualizado_em, data_recebimento, id_forma_pagamento
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ? )
"""

for _, row in df.iterrows():
    cursor.execute(insert_sql, (
    row["id"],
    row["id_parcela"],
    row["vencimento"],
    row["valor_original"],
    row["valor_atual"],
    row["id_situacao"],
    row["criado_em"],
    row["atualizado_em"],
    row["data_recebimento"],
    row["id_forma_pagamento"]
    ))

mssql_conn.commit()
cursor.close()
mssql_conn.close()

print("✔ Dados carregados no SQL Server")
print("🏁 ETL finalizado com sucesso")