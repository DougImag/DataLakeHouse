"""
Pipeline médaillon des données SNCF de fréquentation des gares.

    extract_sncf_frequentation   télécharge l'export CSV (data.sncf.com)
            │
    load_bronze                  CSV brut -> iceberg.bronze.frequentation_sncf_v2
            │
            │   extract_gares_voyageurs   référentiel des gares (position GPS)
            │           │
            │   load_bronze_gares         CSV brut -> iceberg.bronze.gares_voyageurs
            │           │
    transform_silver_with_dbt    dbt build --select source:bronze silver (tests + modèles)
            │
    build_gold_marts             dbt build --select gold   (modèles + tests)

Prérequis : le catalogue Polaris et le schéma bronze existent
(data-platform/setup_lakehouse.sh, à lancer une fois).

Le code du projet est monté dans les conteneurs Airflow sous /opt/dlh
et s'exécute avec le venv /opt/dlh-venv (voir airflow/Dockerfile et
docker-compose.yml). Toutes les tâches peuvent être relancées sans
créer de doublons.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/dlh"
VENV_BIN = "/opt/dlh-venv/bin"
EXTRACTION_DIR = f"{PROJECT_DIR}/extraction-data-api"
RAW_CSV = f"{EXTRACTION_DIR}/data/frequentation_gares_raw.csv"
GARES_RAW_CSV = f"{EXTRACTION_DIR}/data/gares_voyageurs_raw.csv"
DBT_DIR = f"{PROJECT_DIR}/data-platform/dbt"

default_args = {
    "owner": "data",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="sncf_lakehouse_medallion",
    description="Données SNCF de fréquentation des gares : Bronze -> Silver -> Gold",
    doc_md=__doc__,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,  # pas deux chargements concurrents des mêmes tables
    default_args=default_args,
    tags=["sncf", "lakehouse", "medallion"],
) as dag:

    extract = BashOperator(
        task_id="extract_sncf_frequentation",
        doc_md="Télécharge l'export SNCF et produit les fichiers raw / clean dans extraction-data-api/data/.",
        # main.py écrit dans ./data : on se place dans le dossier d'extraction.
        bash_command=f"cd {EXTRACTION_DIR} && {VENV_BIN}/python main.py",
        execution_timeout=timedelta(minutes=10),
    )

    load_bronze = BashOperator(
        task_id="load_bronze",
        doc_md="Charge le CSV brut dans iceberg.bronze.frequentation_sncf_v2 (remplace les lignes du même fichier).",
        bash_command=f"{VENV_BIN}/python {PROJECT_DIR}/data-platform/ingest_bronze.py {RAW_CSV}",
        execution_timeout=timedelta(minutes=20),
    )

    extract_gares = BashOperator(
        task_id="extract_gares_voyageurs",
        doc_md="Télécharge le référentiel SNCF des gares de voyageurs (position GPS) dans extraction-data-api/data/.",
        bash_command=f"{VENV_BIN}/python {EXTRACTION_DIR}/gares.py",
        execution_timeout=timedelta(minutes=10),
    )

    load_bronze_gares = BashOperator(
        task_id="load_bronze_gares",
        doc_md="Charge le référentiel des gares dans iceberg.bronze.gares_voyageurs (remplace les lignes du même fichier).",
        bash_command=(
            f"{VENV_BIN}/python {PROJECT_DIR}/data-platform/ingest_bronze.py "
            f"{GARES_RAW_CSV} --table gares_voyageurs"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    transform_silver = BashOperator(
        task_id="transform_silver_with_dbt",
        doc_md="Teste les sources Bronze, puis construit et teste les modèles Silver (slv_gares_geolocalisation, slv_gares, slv_frequentation_annuelle).",
        bash_command=f"cd {DBT_DIR} && {VENV_BIN}/dbt build --select source:bronze silver",
        execution_timeout=timedelta(minutes=20),
    )

    build_gold = BashOperator(
        task_id="build_gold_marts",
        doc_md="Construit et teste les marts Gold (gares, régions, KPI nationaux).",
        bash_command=f"cd {DBT_DIR} && {VENV_BIN}/dbt build --select gold",
        execution_timeout=timedelta(minutes=20),
    )

    extract >> load_bronze >> transform_silver >> build_gold
    extract_gares >> load_bronze_gares >> transform_silver
