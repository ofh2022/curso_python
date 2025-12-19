from db_connection import get_postgres_connection, get_sqlserver_connection
import pandas as pd


def etl_cliente():
    """
    ETL de Cliente
    Origem: PostgreSQL (geral.*)
    Destino: SQL Server (dw.cliente)
    """

    # =====================
    # 1. EXTRACT
    # =====================
    query = """
        SELECT
            p.id AS id_pessoa,
            CASE
                WHEN pf.id IS NOT NULL THEN 'PF'
                ELSE 'PJ'
            END AS tipo_pessoa,
            pf.cpf,
            pj.cnpj,
            COALESCE(pf.nome, pj.razao_social) AS nome_razao_social,
            e.rua,
            b.descricao AS bairro,
            c.descricao AS cidade,
            uf.sigla AS estado
        FROM geral.pessoa p
        LEFT JOIN geral.pessoa_fisica pf ON pf.id = p.id
        LEFT JOIN geral.pessoa_juridica pj ON pj.id = p.id
        LEFT JOIN geral.endereco e ON e.id_pessoa = p.id
        LEFT JOIN geral.bairro b ON b.id = e.id_bairro
        LEFT JOIN geral.cidade c ON c.id = b.id_cidade
        LEFT JOIN geral.estado uf ON uf.id = c.id_estado;
    """

    pg_conn = get_postgres_connection()
    df = pd.read_sql(query, pg_conn)
    pg_conn.close()

    # =====================
    # 2. TRANSFORM
    # =====================
    df = df.copy() 

    # CPF ou CNPJ em uma única coluna
    df["cpf_cnpj"] = df["cpf"].fillna(df["cnpj"])

    # Remover colunas intermediárias
    df = df.drop(columns=["cpf", "cnpj"])

    # Remover duplicidade por pessoa
    df = df.drop_duplicates(subset=["id_pessoa"])

    # Regra mínima de qualidade
    df = df[df["nome_razao_social"].notnull()]

    # =====================
    # 3. LOAD
    # =====================
    mssql_conn = get_sqlserver_connection()
    cursor = mssql_conn.cursor()

    insert_sql = """
        INSERT INTO dbo.cliente (
            id_pessoa,
            tipo_pessoa,
            cpf_cnpj,
            nome_razao_social,
            rua,
            bairro,
            cidade,
            estado
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    for _, row in df.iterrows():
        cursor.execute(
            insert_sql,
            (
                row["id_pessoa"],
                row["tipo_pessoa"],
                row["cpf_cnpj"],
                row["nome_razao_social"],
                row["rua"],
                row["bairro"],
                row["cidade"],
                row["estado"]
            )
        )

    mssql_conn.commit()
    cursor.close()
    mssql_conn.close()

    print("✔ Dados de cliente carregados no SQL Server")
    print("🏁 ETL de cliente finalizado com sucesso")


if __name__ == "__main__":
    etl_cliente()
