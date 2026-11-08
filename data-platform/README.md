# data-platform

Plateforme Docker Compose du lakehouse SNCF : stockage, catalogue, moteur SQL, transformations, orchestration, BI et gouvernance.

> La procédure complète d'installation (étapes 0 à 17) et le dépannage sont décrits dans le [README à la racine du dépôt](../README.md).

## Services

| Service | Rôle | Port | Profil |
|---|---|---|---|
| `rustfs` | Stockage objet compatible S3 (bucket `lakehouse`, créé par `bucket-init`) | 9000 / 9001 | — |
| `postgres` | Bases `airflow`, `polaris`, `metabase`, `openmetadata_db` (créées par `postgres-init`) | 5433 | — |
| `polaris` | Catalogue Iceberg REST, métadonnées dans Postgres (realm initialisé par `polaris-bootstrap`) | 8181 / 8182 | — |
| `trino` | Moteur SQL, catalogue `iceberg` (`trino/catalog/iceberg.properties`) | 8080 | — |
| `airflow-webserver`, `airflow-scheduler` | Orchestration (image `dlh-airflow`, avec dbt : `airflow/Dockerfile`) | 8081 | — |
| `metabase` (+ `metabase-setup`) | Dashboards sur `iceberg.gold` | 3000 | `bi` |
| `openmetadata` (+ `elasticsearch`, `openmetadata-migrate`) | Catalogue, lignage, tests dbt | 8585 | `gouvernance` |
| `openmetadata-ingest` | Import des métadonnées Trino + dbt, à lancer à la demande | — | `ingest-metadata` |

Iceberg n'est pas un conteneur : c'est le format des tables, géré par Trino et Polaris.

## Contenu du dossier

| Fichier / dossier | Rôle |
|---|---|
| `docker-compose.yml` | Définition de tous les services |
| `.env.example` | Modèle du `.env` (identifiants, profils, paramètres des scripts) |
| `setup_lakehouse.sh` | Crée le catalogue Polaris `lakehouse` et le schéma `iceberg.bronze` (une seule fois) |
| `ingest_bronze.py` | Charge un CSV brut dans `iceberg.bronze` : fréquentation (`frequentation_sncf_v2`, par défaut) ou gares avec position GPS (`--table gares_voyageurs`) |
| `reset_bronze.sh` | ⚠️ Supprime puis recrée la couche Bronze |
| `polaris-token.sh` | Récupère un jeton OAuth Polaris (debug) |
| `trino/catalog/` | Connexion Trino → Polaris / RustFS |
| `airflow/` | Image Airflow du projet et DAG `sncf_lakehouse_medallion` |
| `dbt/` | Projet dbt-trino : couches Silver et Gold ([README](dbt/README.md)) |
| `metabase/` | Initialisation de Metabase (compte admin, connexion Trino) |
| `openmetadata/` | Script d'ingestion des métadonnées dans OpenMetadata |

## Couches médaillon

Toutes les tables sont dans le catalogue Iceberg `lakehouse`, stockées dans le bucket RustFS `lakehouse` :

| Couche | Schéma Trino | Emplacement | Alimentée par |
|---|---|---|---|
| Bronze | `iceberg.bronze` | `s3://lakehouse/bronze/` | `ingest_bronze.py` |
| Silver | `iceberg.silver` | `s3://lakehouse/silver/` | dbt (`dbt/models/silver/`) |
| Gold | `iceberg.gold` | `s3://lakehouse/gold/` | dbt (`dbt/models/gold/`) |

## Démarrage rapide

```bash
cp .env.example .env
docker compose up -d --build                  # + --profile bi / --profile gouvernance si besoin
set -a; eval "$(sed 's/\r$//' .env)"; set +a
export DOCKER_NETWORK=$(docker inspect dlh-trino --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
./setup_lakehouse.sh
python ingest_bronze.py
python ingest_bronze.py ../extraction-data-api/data/gares_voyageurs_raw.csv --table gares_voyageurs
(cd dbt && DBT_PROFILES_DIR=$PWD dbt build)
```

Environnement de développement local : change tous les mots de passe avant un déploiement sur serveur.
