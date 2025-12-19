📊 Projeto de Engenharia de Dados – ETL com Airflow
📌 Visão Geral

Este projeto implementa um pipeline de ETL (Extract, Transform, Load) utilizando Apache Airflow para orquestração, Python para processamento dos dados e Docker para padronização do ambiente.

Os dados são extraídos de um banco PostgreSQL, transformados em Python e carregados em um SQL Server (Data Warehouse).

O ambiente foi desenvolvido utilizando WSL (Ubuntu) para desenvolvimento local e Docker Compose para execução do Airflow.

🏗️ Arquitetura
PostgreSQL (Origem)
        |
        v
   Python ETL
 (Transformações)
        |
        v
SQL Server (DW)
        |
        v
Apache Airflow (Orquestração)

📁 Estrutura do Projeto
.
├── dags/
│   └── etl_cliente_dag.py        # DAG do Airflow
│
├── etl/
│   ├── cliente.py               # ETL de cliente
│   ├── produto.py               # ETL de produto
│   └── vendas.py                # ETL de vendas
│
├── db_connection.py              # Conexões com PostgreSQL e SQL Server
├── docker-compose.yaml           # Infraestrutura do Airflow
├── requirements.txt              # Dependências Python
├── .gitignore                    # Arquivos ignorados pelo Git
├── .env                          # Variáveis de ambiente (NÃO versionado)
└── README.md                     # Documentação do projeto

⚙️ Tecnologias Utilizadas

Python 3.10+

Apache Airflow 3.1

Docker & Docker Compose

PostgreSQL

SQL Server

pyodbc

pandas

WSL (Ubuntu)

🔐 Variáveis de Ambiente

Crie um arquivo .env na raiz do projeto:

# PostgreSQL (Origem)
PG_HOST=localhost
PG_PORT=5432
PG_DB=database_origem
PG_USER=usuario
PG_PASS=senha

# SQL Server (Destino)
MSSQL_HOST=localhost
MSSQL_PORT=1433
MSSQL_DB=dw
MSSQL_USER=sa
MSSQL_PASS=senha


⚠️ Nunca versionar o arquivo .env

🐳 Subindo o Airflow com Docker

Execute os comandos abaixo na pasta onde está o docker-compose.yaml:

docker-compose down
docker-compose up -d


Acesse a interface do Airflow em:

http://localhost:8080


Credenciais padrão:

Usuário: airflow

Senha: airflow

📦 Volume de Código ETL no Airflow

O código ETL local é montado no container via volume:

/home/orlando/curso_python  →  /opt/airflow/projetos


Isso permite que as DAGs importem diretamente os scripts Python sem copiá-los para a pasta dags.

⏱️ Exemplo de DAG (Cliente)
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow/projetos")

from etl.cliente import etl_cliente

with DAG(
    dag_id="etl_cliente_postgres_to_sqlserver",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    PythonOperator(
        task_id="run_etl_cliente",
        python_callable=etl_cliente
    )

🔄 Estratégia de Carga

Staging / Dimensões simples

Carga FULL

TRUNCATE + INSERT

Fatos / Histórico (futuro)

Incremental

MERGE ou controle por data (watermark)

As regras de carga ficam no ETL, nunca na DAG.

📈 Boas Práticas Aplicadas

Separação entre orquestração (Airflow) e lógica de dados (ETL)

Uso de volumes Docker

Uso de PythonOperator

Ambiente isolado com .env

Código versionado sem artefatos locais
