from db_connection import get_postgres_connection, get_sqlserver_connection
import pandas as pd


#extraindo dados do banco de produção

def extract_sales():

    query = f"""
        SELECT id, 
        documento, 
        emissao, 
        vencimento, 
        valor_original, 
        valor_atual, 
        id_situacao, 
        criado_em, 
        atualizado_em, 
        data_pagamento, 
        id_forma_pagamento, 
        descricao
FROM financeiro.conta_pagar;
    """

    conn = get_postgres_connection()
    df = pd.read_sql(query, conn)
    conn.close()

    return df

df = extract_sales()

#print(df.head)


#transformando dados

df = df.copy()

# Garantir tipos de data
df["emissao"] = pd.to_datetime(df["emissao"])
df["vencimento"] = pd.to_datetime(df["vencimento"])
df["criado_em"] = pd.to_datetime(df["criado_em"])

# Remover duplicatas por PK
df = df.drop_duplicates(subset=["id"])

# =====================
# 3. LOAD (SQL Server)
# =====================
mssql_conn = get_sqlserver_connection()
cursor = mssql_conn.cursor()

insert_sql = """
    INSERT INTO dbo.conta_pagar (
        id_titulo, documento, emissao, vencimento, 
        valor_original, valor_atual, id_situacao, 
        criado_em, atualizado_em, data_pagamento, 
        id_forma_pagamento, descricao
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

for _, row in df.iterrows():
    cursor.execute(insert_sql, (
    row["id"],
    row["documento"],
    row["emissao"],
    row["vencimento"],
    row["valor_original"],
    row["valor_atual"],
    row["id_situacao"],
    row["criado_em"],
    row["atualizado_em"],
    row["data_pagamento"],
    row["id_forma_pagamento"],
    row["descricao"]
    ))

mssql_conn.commit()
cursor.close()
mssql_conn.close()

print("✔ Dados carregados no SQL Server")
print("🏁 ETL finalizado com sucesso")