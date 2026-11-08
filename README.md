# DataLakeHouse

## Objectif

Créer une plateforme Data agnostique, flexible, économique, gouvernée et durable, capable de stocker de gros volumes pour des usages IA et BI.

Le projet d'exemple utilise les données publiques de **fréquentation des gares SNCF** (portail [data.sncf.com](https://data.sncf.com)).

## Sommaire

- [Stack technique](#stack-technique) · [Arborescence](#arborescence) · [Architecture](#architecture)
- [Démarrage rapide](#démarrage-rapide)
- [Lancer le projet de A à Z](#lancer-le-projet-de-a-à-z) : étapes 0 à 17
  - Installation : [0. Prérequis](#0-prérequis) · [1. Code](#1-récupérer-le-code) · [2. Python](#2-créer-lenvironnement-python)
  - Données : [3. Extraction](#3-extraire-les-données-sncf) · [7. Init lakehouse](#7-initialiser-le-lakehouse-polaris-trino-schéma-bronze) · [8. Bronze](#8-ingérer-les-données-dans-la-couche-bronze) · [9. Silver / Gold](#9-transformer-en-silver-et-gold-avec-dbt) · [10. Requêtes](#10-interroger-les-données)
  - Plateforme : [4. Configuration](#4-configurer-la-plateforme) · [5. Conteneurs](#5-démarrer-les-conteneurs) · [11. Interfaces web](#11-interfaces-web) · [12. Airflow](#12-orchestration-airflow) · [13. Metabase](#13-dashboards-avec-metabase-optionnel) · [14. OpenMetadata](#14-gouvernance-avec-openmetadata-optionnel)
  - Exploitation : [15. Tests](#15-lancer-les-tests) · [16. Arrêt / reset](#16-arrêter-redémarrer-réinitialiser) · [17. Dépannage](#17-dépannage)
- [Limites connues](#limites-connues)

## Stack technique

| Rôle | Outil | État |
|---|---|---|
| Stockage objet (S3) | RustFS | ✅ |
| Catalogue de données | Apache Polaris (catalogue Iceberg REST) | ✅ |
| Format de table | Apache Iceberg | ✅ |
| Moteur de calcul SQL | Trino | ✅ |
| Extraction | Python (`extraction-data-api/`) | ✅ |
| Ingestion Bronze | Python + Trino (`data-platform/ingest_bronze.py`) | ✅ |
| Transformation Silver / Gold | dbt-trino (`data-platform/dbt/`) | ✅ |
| Orchestration | Apache Airflow (image du projet avec dbt) | ✅ |
| Architecture | Médaillon (Bronze / Silver / Gold) | ✅ |
| Gouvernance | OpenMetadata (catalogue, lignage, tests dbt) | ✅ optionnel (profil `gouvernance`) |
| BI | Metabase | ✅ optionnel (profil `bi`) |

## Arborescence

```
DataLakeHouse/
├── extraction-data-api/        # Extraction des données SNCF (Python)
│   ├── main.py                 # Fréquentation : télécharge le CSV brut puis produit un CSV/Parquet nettoyé
│   ├── gares.py                # Référentiel des gares de voyageurs, avec leur position GPS
│   ├── requirements.txt
│   └── data/                   # Fichiers produits (raw / clean)
├── data-platform/              # Plateforme Docker
│   ├── docker-compose.yml      # RustFS, Polaris, Trino, Postgres, Airflow (+ Metabase, OpenMetadata en option)
│   ├── .env.example            # Modèle du fichier .env (identifiants, profils)
│   ├── setup_lakehouse.sh      # Crée le catalogue Polaris "lakehouse" et le schéma bronze
│   ├── ingest_bronze.py        # Charge le CSV brut dans iceberg.bronze.*
│   ├── reset_bronze.sh         # ⚠️ Supprime puis recrée la couche bronze
│   ├── polaris-token.sh        # Récupère un jeton OAuth Polaris (debug)
│   ├── trino/catalog/          # Configuration du catalogue Trino -> Polaris
│   ├── airflow/Dockerfile      # Image Airflow + venv du projet (pandas, trino, dbt)
│   ├── airflow/dags/           # DAG d'orchestration
│   ├── dbt/                    # Projet dbt-trino : couches Silver et Gold
│   ├── metabase/               # Initialisation de Metabase (compte admin, connexion Trino)
│   └── openmetadata/           # Ingestion des métadonnées Trino + dbt dans OpenMetadata
├── tests/                      # Tests unitaires (pytest)
└── .github/workflows/          # CI GitHub Actions
```

## Architecture

```
API SNCF ──> extraction-data-api/main.py ──> data/frequentation_gares_raw.csv
API SNCF ──> extraction-data-api/gares.py ─> data/gares_voyageurs_raw.csv (position GPS)
                                                     │
                                                     ▼
                                        data-platform/ingest_bronze.py
                                                     │  (INSERT via Trino)
                                                     ▼
   Trino ◄──► Polaris (catalogue Iceberg REST) ◄──► RustFS  s3://lakehouse/bronze/
     │              │
     │              └──> PostgreSQL, base "polaris" (métadonnées du catalogue)
     │
     ├──> dbt : bronze ──> silver ──> gold ──> Metabase (dashboards)
     │
     └──> OpenMetadata : catalogue, descriptions, lignage, tests (Trino + artefacts dbt)
```

### Où sont stockées les données

| Donnée | Emplacement | Persistance |
|---|---|---|
| Fichiers Parquet et métadonnées Iceberg | RustFS, volume Docker `rustfs_data` | ✅ |
| Catalogue Polaris (catalogues, schémas, tables, droits) | PostgreSQL, base `polaris`, volume `postgres_data` | ✅ |
| Métadonnées Airflow | PostgreSQL, base `airflow`, volume `postgres_data` | ✅ |
| DAGs et logs Airflow | `data-platform/airflow/` sur le disque | ✅ |
| Questions et dashboards Metabase | PostgreSQL, base `metabase` | ✅ |
| Catalogue OpenMetadata | PostgreSQL, base `openmetadata_db` + index Elasticsearch (volume `elasticsearch_data`) | ✅ |
| Artefacts dbt du DAG (`manifest.json`…) | volume Docker `dbt_artifacts` | ✅ |
| Fichiers d'extraction | `extraction-data-api/data/` sur le disque | ✅ |

Les données survivent à `docker compose stop`, `restart` et `down`. **Seul `docker compose down -v` les efface**, car il supprime les volumes.

## Démarrage rapide

Pour qui connaît déjà Docker et dbt. Chaque commande est détaillée dans les étapes 0 à 17 ci-dessous.

```bash
# Python (depuis la racine)
python3 -m venv .venv && source .venv/bin/activate
pip install -r extraction-data-api/requirements.txt -r data-platform/dbt/requirements.txt pytest

# Extraction (main.py à lancer DEPUIS extraction-data-api/)
(cd extraction-data-api && python main.py)
python extraction-data-api/gares.py              # gares + position GPS

# Plateforme
cd data-platform
cp .env.example .env
docker compose up -d --build                     # attendre que tout soit healthy : docker compose ps
set -a; eval "$(sed 's/\r$//' .env)"; set +a
export DOCKER_NETWORK=$(docker inspect dlh-trino --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
./setup_lakehouse.sh                             # une seule fois
python ingest_bronze.py                          # Bronze : fréquentation
python ingest_bronze.py ../extraction-data-api/data/gares_voyageurs_raw.csv --table gares_voyageurs
(cd dbt && DBT_PROFILES_DIR=$PWD dbt build)      # Silver + Gold, avec les tests
```

Vérification : `docker exec -it dlh-trino trino --execute "SELECT * FROM iceberg.gold.gld_kpi_national ORDER BY annee"`.

---

# Lancer le projet de A à Z

Les commandes ci-dessous sont à exécuter **depuis la racine du dépôt**, sauf mention contraire. Elles ont été écrites pour macOS et Linux (bash/zsh). Sous Windows, utilise Git Bash ou WSL.

## 0. Prérequis

| Outil | Version conseillée | Vérification |
|---|---|---|
| Git | toute version récente | `git --version` |
| Docker Desktop (ou Docker Engine) + Compose v2 | ≥ 24 | `docker compose version` |
| Python | 3.12 | `python3 --version` |
| curl | — | `curl --version` |
| jq | — | `jq --version` (macOS : `brew install jq`) |

Prévois au moins **6 Go de RAM** pour Docker : Trino et Airflow sont gourmands. Avec Metabase (+1 Go) et OpenMetadata (+2,5 Go avec Elasticsearch), compte **8 Go** ou plus. Avec moins de RAM, active les deux profils l'un après l'autre.

Ports utilisés : `8080` (Trino), `8081` (Airflow), `8181`/`8182` (Polaris), `9000`/`9001` (RustFS), `5433` (Postgres), `3000` (Metabase), `8585` (OpenMetadata).

## 1. Récupérer le code

```bash
git clone <url-du-depot> DataLakeHouse
cd DataLakeHouse
```

## 2. Créer l'environnement Python

Un seul environnement virtuel, à la racine, sert pour l'extraction, l'ingestion, dbt et les tests :

```bash
python3 -m venv .venv
source .venv/bin/activate            # Windows Git Bash : source .venv/Scripts/activate

pip install --upgrade pip
pip install -r extraction-data-api/requirements.txt
pip install -r data-platform/dbt/requirements.txt     # dbt-core + dbt-trino
pip install pytest                    # uniquement pour lancer les tests
```

> **macOS, Python téléchargé sur python.org** : si l'installation de dbt échoue avec `CERTIFICATE_VERIFY_FAILED`, lance une fois `"/Applications/Python 3.14/Install Certificates.command"` (adapte la version), puis relance le `pip install`.

`requirements.txt` contient tout ce dont les scripts ont besoin : `pandas`, `requests` (téléchargement), `pyarrow` (écriture Parquet) et `trino` (client SQL utilisé par `ingest_bronze.py`).

## 3. Extraire les données SNCF

Le script écrit dans `./data`, relativement au dossier courant. Il faut donc le lancer **depuis `extraction-data-api/`**. Lancé depuis la racine, il crée un dossier `data/` à la racine, que ni `ingest_bronze.py` ni le DAG ne liront :

```bash
cd extraction-data-api
python main.py
cd ..
```

Fichiers produits dans `extraction-data-api/data/` :

- `frequentation_gares_raw.csv` : export brut de l'API, séparateur `;`, environ 3 000 gares. **C'est ce fichier qui alimente la couche Bronze.**
- `frequentation_gares_clean.csv` et `.parquet` : version nettoyée, avec une ligne par gare et par année (environ 30 000 lignes). Colonnes : `nom_gare`, `code_uic`, `code_postal`, `direction_regionale`, `segmentation_drg`, `segmentation_marketing`, `indicateur_annee`, `voyageurs`, `annee`, `date_extraction_utc`. Il n'y a jamais deux lignes avec le même couple (`code_uic`, `annee`).

### Position GPS des gares

Le jeu de fréquentation ne contient pas de coordonnées. Elles viennent du référentiel [Gares de voyageurs](https://ressources.data.sncf.com/explore/dataset/gares-de-voyageurs/information/) (SNCF Gares & Connexions, licence **ODbL**, mis à jour chaque jour), récupéré par `gares.py`. Ce script construit son chemin de sortie à partir de son propre emplacement : il peut être lancé depuis n'importe quel dossier.

```bash
python extraction-data-api/gares.py
```

Il produit `extraction-data-api/data/gares_voyageurs_raw.csv` : l'export brut, séparateur `;`, environ 2 800 gares. Colonnes : `nom`, `libellecourt` (trigramme), `segment_drg`, `position_geographique`, `codeinsee`, `codes_uic`, `id`. Avant d'écrire le fichier, le script vérifie les colonnes, le nombre de gares (au moins 2 000) et que 95 % des gares au moins ont une position exploitable. Sinon, il s'arrête sans écraser le fichier précédent.

Pièges de cet export, gérés dans dbt (étape 9) :

- `position_geographique` est une seule chaîne `"<latitude>, <longitude>"`, avec **la latitude en premier** (l'inverse du GeoJSON) ;
- `codes_uic` et `segment_drg` peuvent contenir plusieurs valeurs séparées par `;` (ex. `87271494;87001479`, `A;B`).

Le lien avec la fréquentation se fait par le **code UIC**. Environ 92 % des gares de fréquentation figurent dans ce référentiel. Les autres (~230) n'ont pas de coordonnées : leur latitude et leur longitude valent `NULL`.

## 4. Configurer la plateforme

```bash
cd data-platform
```

Crée le fichier `data-platform/.env` à partir du modèle. Le `.env` est ignoré par Git :

```bash
cp .env.example .env
```

Les valeurs par défaut conviennent pour un usage local. Les variables principales sont :

| Variable | Rôle |
|---|---|
| `RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY` | Identifiants S3 de RustFS, utilisés aussi par Polaris, Trino et les scripts |
| `AIRFLOW_DB_PASSWORD` | Mot de passe Postgres, utilisé par Airflow **et** Polaris |
| `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` | Compte administrateur de l'interface Airflow |
| `COMPOSE_PROFILES` | Services optionnels lancés à chaque `up` : `bi` (Metabase), `gouvernance` (OpenMetadata) |
| `METABASE_ADMIN_EMAIL` / `METABASE_ADMIN_PASSWORD` | Compte administrateur Metabase, créé au premier démarrage (étape 13) |
| `OPENMETADATA_ADMIN_PASSWORD` | Mot de passe de `admin@open-metadata.org`, utilisé par l'ingestion (étape 14) |

`.env.example` contient aussi, en commentaire, les paramètres facultatifs des scripts (`TRINO_PROPS`, `CATALOG`, `BUCKET`, `TRINO_HOST`…). Pour en modifier un, décommente sa ligne.

> Les identifiants Polaris (`root` / `s3cr3t`) sont pour l'instant écrits en dur dans `docker-compose.yml` (service `polaris-bootstrap`) et dans `trino/catalog/iceberg.properties`. Si tu les changes, modifie les deux fichiers. Ils ne sont pris en compte qu'au **premier** bootstrap : pour les changer ensuite, il faut repartir d'une base `polaris` vide.
>
> Le mot de passe Postgres n'est appliqué qu'à la création du volume `postgres_data`. Le modifier plus tard dans `.env` ne change pas celui de la base existante.

## 5. Démarrer les conteneurs

```bash
docker compose config --quiet          # valide la syntaxe du compose
docker compose up -d --build           # --build : construit l'image Airflow du projet
docker compose ps
```

Au premier lancement, Docker construit l'image `dlh-airflow:2.10.5` (fichier `airflow/Dockerfile`). C'est l'image Airflow officielle, avec en plus un venv `/opt/dlh-venv` qui contient les dépendances du projet (pandas, trino, dbt). Le build prend 1 à 2 minutes. Relance `docker compose up -d --build` après chaque modification de `extraction-data-api/requirements.txt` ou de `dbt/requirements.txt`.

Les conteneurs portent le préfixe `dlh-` (`dlh-trino`, `dlh-polaris`, `dlh-postgres`…), pour ne pas entrer en conflit avec ceux d'autres projets. Avec `docker compose`, on utilise le **nom du service**, sans préfixe (`docker compose logs polaris`). Avec `docker` directement, on utilise le **nom du conteneur** (`docker exec dlh-trino …`). Entre eux, les conteneurs se joignent par leur nom de service (`http://polaris:8181`, `http://rustfs:9000`, `postgres:5432`).

Ordre de démarrage automatique :

1. `rustfs`, puis `bucket-init`, qui crée le bucket `lakehouse` ;
2. `postgres`, puis `postgres-init`, qui crée les bases `polaris` (avec le schéma `polaris_schema`), `metabase` et `openmetadata_db` ;
3. `polaris-bootstrap` : il crée les tables Polaris et le principal `root`. S'il est relancé alors que le realm est déjà initialisé, il ne fait rien ;
4. `polaris`, puis `trino` ;
5. en parallèle de Polaris : `airflow-init`, puis `airflow-webserver` et `airflow-scheduler`.

`bucket-init`, `postgres-init`, `polaris-bootstrap` et `airflow-init` s'arrêtent une fois leur travail fini. L'état `exited (0)` est donc normal pour eux.

Attends que `rustfs`, `polaris` et `airflow-webserver` soient en état `healthy`. Compte 2 à 5 minutes au premier lancement, le temps de télécharger et de construire les images :

```bash
docker compose ps
docker compose logs -f polaris trino   # Ctrl+C pour quitter
```

Contrôles rapides :

```bash
curl -s http://localhost:8182/q/health | jq .status     # Polaris -> "UP"
curl -s http://localhost:8080/v1/info | jq .starting    # Trino   -> false
docker logs dlh-bucket-init                                  # doit lister le bucket "lakehouse"
```

## 6. Vérifier le compte administrateur Airflow

Le service `airflow-init` crée automatiquement le compte défini par `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` dans `.env`. Pour le vérifier :

```bash
docker compose logs airflow-init | grep "Compte admin"   # "créé" ou "déjà présent"
docker compose exec airflow-webserver airflow users list
```

> Le compte n'est créé qu'une seule fois. Si tu changes `AIRFLOW_ADMIN_PASSWORD` par la suite, le mot de passe existant reste le même. Pour le changer :
>
> ```bash
> docker compose exec airflow-webserver airflow users delete --username admin
> docker compose up -d airflow-init      # recrée le compte avec les valeurs du .env
> ```

## 7. Initialiser le lakehouse (Polaris, Trino, schéma bronze)

Toujours depuis `data-platform/` :

```bash
# Charge les variables du .env dans le shell (le sed retire d'éventuels \r Windows)
set -a; eval "$(sed 's/\r$//' .env)"; set +a

# Réseau Docker utilisé par le conteneur trino
export DOCKER_NETWORK=$(docker inspect dlh-trino --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
echo "$DOCKER_NETWORK"                 # ex. data-platform_default

chmod +x setup_lakehouse.sh reset_bronze.sh polaris-token.sh
./setup_lakehouse.sh
```

> Le script fonctionne sous macOS, Linux et Git Bash. Par défaut, il modifie `trino/catalog/iceberg.properties`, situé à côté de lui. Pour viser un autre fichier : `export TRINO_PROPS=/chemin/vers/iceberg.properties`.
>
> Sous Git Bash (Windows), ajoute aussi `export MSYS_NO_PATHCONV=1`.

Le script :

1. récupère un jeton OAuth2 auprès de Polaris ;
2. crée le bucket `lakehouse` dans RustFS s'il n'existe pas encore ;
3. crée le catalogue Polaris `lakehouse`, qui pointe vers `s3://lakehouse/` ;
4. donne les droits `CATALOG_MANAGE_CONTENT` au rôle `service_admin` ;
5. désactive le *credential vending* dans `trino/catalog/iceberg.properties` (RustFS n'a pas de STS), puis redémarre Trino ;
6. crée le schéma `iceberg.bronze`.

À la fin, `SHOW SCHEMAS FROM iceberg` doit afficher `bronze`.

## 8. Ingérer les données dans la couche Bronze

Depuis `data-platform/`, avec le venv activé :

```bash
python ingest_bronze.py
```

Par défaut, le script :

- lit `../extraction-data-api/data/frequentation_gares_raw.csv` (un autre chemin peut être passé en argument) ;
- crée la table `iceberg.bronze.frequentation_sncf_v2` à partir des en-têtes du CSV, toutes colonnes en `VARCHAR`, plus `source_fichier` et `date_ingestion` ;
- supprime les lignes déjà chargées depuis ce même fichier puis les réinsère. Il peut donc être relancé sans créer de doublons.

Le référentiel des gares se charge avec le même script, dans sa propre table :

```bash
python ingest_bronze.py ../extraction-data-api/data/gares_voyageurs_raw.csv --table gares_voyageurs
```

Résultat : `iceberg.bronze.gares_voyageurs` (environ 2 800 lignes, tout en `VARCHAR`, comme la fréquentation).

Options utiles :

```bash
python ingest_bronze.py --help
python ingest_bronze.py chemin/vers/fichier.csv --table ma_table --batch-size 500
python ingest_bronze.py --no-replace-source     # ajoute les lignes sans supprimer l'existant
```

Variables d'environnement reconnues : `TRINO_HOST` (localhost), `TRINO_PORT` (8080), `TRINO_USER` (ingest), `TRINO_CATALOG` (iceberg), `TRINO_SCHEMA` (bronze), `BRONZE_TABLE` (frequentation_sncf_v2).

## 9. Transformer en Silver et Gold avec dbt

Depuis `data-platform/`, avec le venv activé :

```bash
cd dbt
export DBT_PROFILES_DIR=$PWD       # utilise le profiles.yml du projet
dbt debug                          # vérifie la connexion à Trino
dbt build                          # construit les modèles ET lance les tests
cd ..
```

Résultat : 6 tables et 45 tests. Un 6e modèle, `int_bronze_derniere_ingestion`, est éphémère : dbt l'intègre aux requêtes Silver sans créer de table. Il ne garde que la dernière ingestion de chaque gare.

| Couche | Table | Contenu |
|---|---|---|
| Silver | `iceberg.silver.slv_gares_geolocalisation` | Position GPS des gares, une ligne par code UIC : `latitude` et `longitude` en `double`, code commune INSEE, segment DRG |
| Silver | `iceberg.silver.slv_gares` | Référentiel des gares, une ligne par gare : colonnes typées, chaînes vides remplacées par `NULL`, code postal sur 5 chiffres, département, **latitude, longitude, code commune** |
| Silver | `iceberg.silver.slv_frequentation_annuelle` | Une ligne par gare et par année (colonnes annuelles passées en lignes, nombres en `bigint`) |
| Gold | `iceberg.gold.gld_frequentation_gares` | Mart principal du dashboard : gare × année, évolution sur un an, rang national, part nationale, **latitude, longitude** |
| Gold | `iceberg.gold.gld_frequentation_regions` | Agrégat par direction régionale et par année |
| Gold | `iceberg.gold.gld_kpi_national` | KPI nationaux par année, dont l'écart avec 2019 (année pré-Covid) |

Commandes utiles :

```bash
dbt build --select silver          # une seule couche (tags silver / gold)
dbt build --select +gld_kpi_national   # un modèle et tout ce dont il dépend
dbt test                           # uniquement les tests
dbt build --vars '{annee_reference: 2023}'   # change l'année de référence des KPI
dbt docs generate && dbt docs serve --port 8085   # documentation et lignage sur http://localhost:8085
```

Les détails (règles de nettoyage, tests, conventions) sont décrits dans [`data-platform/dbt/README.md`](data-platform/dbt/README.md).

## 10. Interroger les données

```bash
docker exec -it dlh-trino trino --execute "SHOW TABLES FROM iceberg.bronze"
docker exec -it dlh-trino trino --execute "SELECT count(*) FROM iceberg.bronze.frequentation_sncf_v2"
docker exec -it dlh-trino trino --execute "SHOW TABLES FROM iceberg.gold"

# Top 10 des gares en 2024
docker exec -it dlh-trino trino --execute "
  SELECT rang_national, nom_gare, nb_voyageurs, evolution_pct
  FROM iceberg.gold.gld_frequentation_gares
  WHERE annee = 2024
  ORDER BY rang_national
  LIMIT 10"

# KPI nationaux
docker exec -it dlh-trino trino --execute "SELECT * FROM iceberg.gold.gld_kpi_national ORDER BY annee"
```

Pour une session SQL interactive :

```bash
docker exec -it dlh-trino trino --catalog iceberg --schema gold
```

Les fichiers Parquet et les métadonnées Iceberg sont visibles dans RustFS, sous `s3://lakehouse/bronze/`, `s3://lakehouse/silver/` et `s3://lakehouse/gold/`.

## 11. Interfaces web

| Service | URL | Identifiants |
|---|---|---|
| RustFS (console S3) | http://localhost:9001 | `RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY` |
| Trino UI | http://localhost:8080 | n'importe quel nom d'utilisateur |
| Airflow | http://localhost:8081 | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` (défaut `admin` / `admin`) |
| Polaris API | http://localhost:8181 | OAuth `root` / `s3cr3t` |
| Polaris santé | http://localhost:8182/q/health | — |
| Metabase (profil `bi`) | http://localhost:3000 | `METABASE_ADMIN_EMAIL` / `METABASE_ADMIN_PASSWORD` (défaut `admin@datalakehouse.local` / `Lakehouse2026!`) |
| OpenMetadata (profil `gouvernance`) | http://localhost:8585 | `admin@open-metadata.org` / `admin` |

## 12. Orchestration Airflow

Le DAG `sncf_lakehouse_medallion` (`data-platform/airflow/dags/sncf_lakehouse.py`) automatise les étapes 3, 8 et 9 :

```
extract_sncf_frequentation >> load_bronze ───────┐
                                                 ├──> transform_silver_with_dbt >> build_gold_marts
extract_gares_voyageurs >> load_bronze_gares ────┘
```

| Tâche | Commande exécutée dans le conteneur | Équivalent manuel |
|---|---|---|
| `extract_sncf_frequentation` | `python main.py` (dans `extraction-data-api/`) | étape 3 |
| `load_bronze` | `python ingest_bronze.py …/frequentation_gares_raw.csv` | étape 8 |
| `extract_gares_voyageurs` | `python gares.py` | étape 3 |
| `load_bronze_gares` | `python ingest_bronze.py …/gares_voyageurs_raw.csv --table gares_voyageurs` | étape 8 |
| `transform_silver_with_dbt` | `dbt build --select source:bronze silver` | étape 9 |
| `build_gold_marts` | `dbt build --select gold` | étape 9 |

Fonctionnement :

- **Planification :** tous les jours (`@daily`), sans rattrapage des jours passés (`catchup=False`), et jamais deux runs en même temps (`max_active_runs=1`).
- **Relance :** chaque tâche est réessayée 2 fois, à 5 minutes d'intervalle, avant d'être déclarée en échec. Le DAG peut être relancé sans créer de doublons.
- **Contrôle qualité :** les tests dbt font partie des tâches. Si un test Silver échoue, Gold n'est pas reconstruit.
- **Code exécuté :** les fichiers du dépôt sont montés dans les conteneurs Airflow sous `/opt/dlh`. Une modification du code ou des modèles dbt est donc prise en compte au run suivant, sans reconstruire l'image.
- ⚠️ La tâche d'extraction **réécrit** les fichiers de `extraction-data-api/data/` sur ton disque.

**Prérequis :** l'étape 7 (`setup_lakehouse.sh`) doit avoir été faite une fois.

Pour le déclencher à la main (il est en pause par défaut) :

```bash
cd data-platform
docker compose exec airflow-scheduler airflow dags unpause sncf_lakehouse_medallion
docker compose exec airflow-scheduler airflow dags trigger sncf_lakehouse_medallion

# Suivi
docker compose exec airflow-scheduler airflow dags list-runs -d sncf_lakehouse_medallion
```

Tu peux aussi le faire depuis l'interface (http://localhost:8081) : active le DAG avec l'interrupteur, puis clique sur ▶ *Trigger DAG*. Les logs de chaque tâche sont visibles dans la vue *Grid*.

> **Linux / VPS :** ajoute `AIRFLOW_UID=$(id -u)` dans `.env` avant le premier `up`. Sinon, les fichiers écrits par Airflow dans les dossiers montés (logs, données d'extraction) appartiennent à l'utilisateur `50000` du conteneur. Sur macOS, ce n'est pas nécessaire.

## 13. Dashboards avec Metabase (optionnel)

Metabase se lance avec le profil Compose `bi` :

```bash
cd data-platform
docker compose --profile bi up -d
docker compose logs metabase-setup      # "Metabase prêt : http://localhost:3000"
```

Au premier démarrage, le service `metabase-setup` configure Metabase automatiquement :

- il crée le compte administrateur (`METABASE_ADMIN_EMAIL` / `METABASE_ADMIN_PASSWORD`, à définir dans `.env`) ;
- il ajoute la connexion **« Lakehouse (Trino) »** vers `iceberg.gold`, avec le pilote *Starburst* de Metabase, compatible Trino.

S'il est relancé, il ne recrée rien. Ouvre http://localhost:3000, connecte-toi, puis :

1. *Parcourir les données → Lakehouse (Trino)* : les tables `gld_kpi_national`, `gld_frequentation_gares` et `gld_frequentation_regions` y sont listées ;
2. *Nouveau → Question* ou *Requête SQL* pour construire les graphiques ;
3. *Nouveau → Tableau de bord* pour les assembler.

Idées pour un premier dashboard :

| Visuel | Table | Réglage |
|---|---|---|
| Tuiles KPI (voyageurs 2024, évolution, écart avec 2019) | `gld_kpi_national` | filtre `annee = 2024` |
| Courbe de fréquentation 2015-2024 (on voit la chute du Covid) | `gld_kpi_national` | `annee` en X, `nb_voyageurs` en Y |
| Top 20 des gares | `gld_frequentation_gares` | filtre sur l'année, tri par `rang_national` |
| Répartition par région | `gld_frequentation_regions` | barres, `part_nationale_pct` |
| Carte par département | `gld_frequentation_gares` | `code_departement`, somme de `nb_voyageurs` |
| Carte des gares (points) | `gld_frequentation_gares` | filtre sur l'année, visualisation *Carte → Épingles*, colonnes `latitude` / `longitude`, taille selon `nb_voyageurs` |
| Carte de la fréquentation par ville | `gld_frequentation_gares` | requête SQL ci-dessous |

### Exemple : carte de la fréquentation par ville

Cette question affiche, pour la dernière année disponible, la somme des voyageurs de chaque ville, placée sur une carte.

1. *Nouveau → Requête SQL*, base **Lakehouse (Trino)**, colle la requête ci-dessous, puis ▶ ;
2. *Visualisation → Carte*, type **Épingles**, avec `latitude` et `longitude` comme coordonnées ;
3. enregistre la question, puis ajoute-la à un tableau de bord.

```sql
WITH gares AS (
    SELECT
        -- Paris, Lyon et Marseille sont découpées en arrondissements : on les regroupe
        CASE
            WHEN code_commune LIKE '751%' THEN '75056'
            WHEN code_commune BETWEEN '69381' AND '69389' THEN '69123'
            WHEN code_commune BETWEEN '13201' AND '13216' THEN '13055'
            ELSE code_commune
        END AS code_commune,
        nom_gare,
        nb_voyageurs,
        latitude,
        longitude
    FROM iceberg.gold.gld_frequentation_gares
    WHERE annee = (SELECT max(annee) FROM iceberg.gold.gld_frequentation_gares)
      AND latitude IS NOT NULL
)
SELECT
    code_commune,
    -- Nom affiché : la gare la plus fréquentée de la ville
    max_by(nom_gare, nb_voyageurs)                                  AS ville,
    count(*)                                                        AS nb_gares,
    sum(nb_voyageurs)                                               AS nb_voyageurs,
    -- Coordonnées de la ville : barycentre des gares, pondéré par la fréquentation
    -- (moyenne simple si toutes les gares de la ville ont 0 voyageur)
    round(coalesce(sum(latitude * nb_voyageurs)  / nullif(sum(nb_voyageurs), 0), avg(latitude)), 6)  AS latitude,
    round(coalesce(sum(longitude * nb_voyageurs) / nullif(sum(nb_voyageurs), 0), avg(longitude)), 6) AS longitude
FROM gares
GROUP BY code_commune
ORDER BY nb_voyageurs DESC
```

Résultat (environ 2 460 villes pour 2024) :

| code_commune | ville | nb_gares | nb_voyageurs | latitude | longitude |
|---|---|---|---|---|---|
| 75056 | Paris Gare du Nord | 27 | 830 357 575 | 48.86661 | 2.346914 |
| 93066 | Saint-Denis | 3 | 59 625 499 | 48.928908 | 2.350702 |
| 69123 | Lyon Part Dieu | 6 | 55 563 984 | 45.758941 | 4.851264 |

Limites de cet exemple :

- les données ne contiennent pas le nom des villes, seulement leur code INSEE (`code_commune`) : la colonne `ville` affiche le nom de la gare la plus fréquentée de la commune ;
- les coordonnées de la ville sont celles du barycentre de ses gares, pondéré par la fréquentation, pas celles du centre officiel de la commune ;
- les gares sans position GPS (environ 8 %, voir l'étape 3) sont exclues, leur fréquentation n'est donc pas comptée.

> Si les colonnes `latitude` / `longitude` n'apparaissent pas dans Metabase après une mise à jour des modèles dbt : *Admin → Bases de données → Lakehouse (Trino) → Synchroniser le schéma*.

Les questions et dashboards sont enregistrés dans la base Postgres `metabase` : ils survivent aux redémarrages.

> Pour activer Metabase en permanence, sans ajouter `--profile bi` à chaque commande, mets `COMPOSE_PROFILES=bi` dans `.env` (ou `COMPOSE_PROFILES=bi,gouvernance` pour activer les deux).

## 14. Gouvernance avec OpenMetadata (optionnel)

OpenMetadata se lance avec le profil `gouvernance`. Il ajoute trois services : `openmetadata` (serveur et interface web), `elasticsearch` (moteur de recherche) et `openmetadata-migrate` (création du schéma de sa base).

```bash
cd data-platform
docker compose --profile gouvernance up -d
docker compose --profile gouvernance ps     # attendre que "openmetadata" soit healthy (2 à 3 min)
```

Ensuite, importe les métadonnées du lakehouse. Il faut que la Gold soit construite (étape 9 ou DAG Airflow) :

```bash
docker compose --profile gouvernance run --rm openmetadata-ingest
```

Le script `openmetadata/ingest.py` :

1. récupère le jeton du compte de service `ingestion-bot` ;
2. **ingère Trino :** il crée le service `lakehouse_trino` et importe les schémas `bronze`, `silver` et `gold`, avec leurs tables et colonnes ;
3. **ingère dbt :** il importe les descriptions des modèles et des colonnes, les 45 tests et le **lignage** `bronze.frequentation_sncf_v2` / `bronze.gares_voyageurs → silver.* → gold.*`. Il prend le `manifest.json` le plus récent entre celui produit par le DAG Airflow (volume `dbt_artifacts`) et celui d'un `dbt build` lancé à la main (`dbt/target/`).

Tu peux le relancer après chaque modification des modèles dbt : OpenMetadata met à jour les entités existantes.

Ouvre http://localhost:8585 (`admin@open-metadata.org` / `admin`) :

- *Explore → Tables* : les 8 tables, avec leurs descriptions et colonnes ;
- onglet *Lineage* d'une table Gold : la chaîne Bronze → Silver → Gold ;
- onglet *Data Observability* : les tests dbt et leur dernier résultat.

> Pour que l'ingestion continue de fonctionner, change le mot de passe admin d'OpenMetadata dans l'interface **et** mets la même valeur dans `OPENMETADATA_ADMIN_PASSWORD` (`.env`).
>
> On ne déploie pas le conteneur d'ingestion officiel d'OpenMetadata, car il embarque un second Airflow. Les ingestions se lancent donc depuis la ligne de commande, pas depuis l'interface d'OpenMetadata.

## 15. Lancer les tests

Depuis la racine, avec le venv activé :

```bash
python -m pytest tests/ -v
```

La CI GitHub Actions (`.github/workflows/python-ci.yml`) vérifie la syntaxe de tous les fichiers `.py` puis lance ces tests. Elle se déclenche sur chaque push ou pull request vers `main` et `develop`.

## 16. Arrêter, redémarrer, réinitialiser

```bash
cd data-platform

docker compose stop            # arrête les conteneurs (les données sont conservées)
docker compose start           # les redémarre

docker compose down            # supprime les conteneurs, conserve les volumes
docker compose down -v         # ⚠️ supprime aussi les volumes : RustFS, Postgres, Elasticsearch… sont effacés
```

> Les services des profils `bi` et `gouvernance` ne sont concernés que si le profil est actif. Ajoute `--profile '*'` (ex. `docker compose --profile '*' down`) ou définis `COMPOSE_PROFILES` dans `.env`.

Le catalogue Polaris est stocké dans PostgreSQL. Après un `stop`/`start`, un `restart` ou un `down`/`up`, les tables restent visibles dans Trino : **inutile de relancer les étapes 7, 8 et 9**.

Pour repartir de zéro (⚠️ efface RustFS, le catalogue Polaris et Airflow), puis tout recréer :

```bash
docker compose down -v
docker compose up -d
# puis les étapes 7, 8 et 9 (le compte Airflow est recréé automatiquement)
```

Réinitialiser la couche Bronze (⚠️ **destructif** : supprime toutes les tables `iceberg.bronze.*` et le préfixe `s3://lakehouse/bronze/`) :

```bash
set -a; eval "$(sed 's/\r$//' .env)"; set +a
export DOCKER_NETWORK=$(docker inspect dlh-trino --format '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
./reset_bronze.sh
python ingest_bronze.py        # recharge les données
(cd dbt && DBT_PROFILES_DIR=$PWD dbt build)   # reconstruit Silver et Gold
```

> `reset_bronze.sh` recrée aussi une table `frequentation_sncf`, dont les colonnes ne correspondent pas au CSV SNCF. La vraie table est `frequentation_sncf_v2`, créée par `ingest_bronze.py`.

## 17. Dépannage

| Symptôme | Piste |
|---|---|
| `Token invalide` dans `setup_lakehouse.sh` | Polaris n'est pas encore prêt : `docker compose logs polaris`, puis attendre l'état `healthy`. |
| `exporte RUSTFS_ACCESS_KEY` / `exporte DOCKER_NETWORK` | Les variables du `.env` n'ont pas été chargées dans le shell (étape 7). |
| Airflow redémarre en boucle ou affiche une erreur d'authentification Postgres | `AIRFLOW_DB_PASSWORD` a été modifié dans `.env` après la création du volume `postgres_data`. Remets l'ancienne valeur, ou repars de zéro avec `docker compose down -v` puis `up -d` (⚠️ efface les données). |
| Impossible de se connecter à Airflow | `docker compose logs airflow-init` : le compte doit apparaître comme « créé » ou « déjà présent ». Si le mot de passe a changé dans `.env` après la création, voir l'étape 6. |
| `ingest_bronze.py` : « Le schéma de la table ne correspond pas au CSV » | Les colonnes du CSV ont changé : relance avec `--table frequentation_sncf_v3`. |
| `Access Denied` / erreur S3 dans Trino | Vérifie que `RUSTFS_ACCESS_KEY`/`SECRET_KEY` sont les mêmes pour RustFS et Trino, puis `docker restart dlh-trino`. |
| `polaris` ne démarre pas, `polaris-bootstrap` en erreur | `docker compose logs postgres-init polaris-bootstrap`. Vérifie que `AIRFLOW_DB_PASSWORD` correspond au mot de passe avec lequel le volume `postgres_data` a été créé. |
| Le catalogue `lakehouse` a disparu | Le volume `postgres_data` a été supprimé (`down -v`) : relance les étapes 7, 8 et 9. |
| `Conflict. The container name "/dlh-…" is already in use` | Un ancien conteneur de ce projet traîne encore : `docker compose down`, ou `docker rm dlh-<service>`. |
| `dbt` : « Could not find profile named 'sncf_lakehouse' » | `DBT_PROFILES_DIR` n'est pas défini : `export DBT_PROFILES_DIR=$PWD` depuis `data-platform/dbt/`. |
| `dbt` : « Table 'iceberg.bronze.frequentation_sncf_v2' does not exist » | La couche Bronze n'est pas chargée : relance l'étape 8. |
| Tâche `load_bronze` ou dbt en échec : « Schema 'bronze' does not exist » / erreur de connexion à Trino | L'étape 7 n'a pas été faite, ou Trino n'est pas démarré : `docker compose ps trino`. |
| Tâche `extract_sncf_frequentation` en échec : `PermissionError` | Linux : `AIRFLOW_UID` manque dans `.env` (voir l'étape 12), puis `docker compose up -d`. |
| `metabase-setup` en échec : HTTP 400 sur `/api/setup` (mot de passe refusé) | `METABASE_ADMIN_PASSWORD` est trop simple pour Metabase : choisis un mot de passe plus long, avec chiffres et symboles. |
| `openmetadata` ne devient pas healthy / conteneurs arrêtés sans message (code 137) | Docker manque de RAM : augmente la mémoire de Docker Desktop, ou baisse `OPENMETADATA_HEAP_OPTS` / `ELASTICSEARCH_JAVA_OPTS` dans `.env`. |
| `openmetadata-ingest` : « Aucun manifest.json dbt trouvé » | Lance d'abord le DAG Airflow, ou `dbt build` dans `data-platform/dbt/` (étape 9). |
| `openmetadata-ingest` : HTTP 401 sur `/users/login` | Le mot de passe admin d'OpenMetadata a changé : mets-le à jour dans `OPENMETADATA_ADMIN_PASSWORD`. |
| Un dossier `data/` apparaît à la racine du dépôt / `ingest_bronze.py` ne trouve pas le CSV | `main.py` a été lancé depuis la racine. Relance-le depuis `extraction-data-api/` (étape 3), puis supprime le `data/` de la racine. |
| `Bind for 0.0.0.0:8080 failed: port is already allocated` | Un autre projet utilise déjà ce port (voir la liste des ports à l'étape 0) : arrête-le avec `docker ps`, puis `docker stop <nom>`. |

Commandes utiles :

```bash
docker compose logs -f <service>         # rustfs | polaris | trino | airflow-webserver | airflow-scheduler
source ./polaris-token.sh && echo "$TOKEN"
curl -s -H "Authorization: Bearer $TOKEN" http://localhost:8181/api/management/v1/catalogs | jq
```

---

## Limites connues

Ces points sont acceptables pour un environnement de développement local, mais à traiter avant un déploiement sur un serveur :

| Limite | Conséquence | Piste |
|---|---|---|
| Identifiants Polaris `root` / `s3cr3t` écrits en dur (`docker-compose.yml` et `trino/catalog/iceberg.properties`) | Même secret partout, et visible dans le dépôt | Les passer en variables du `.env`, puis créer un principal dédié à Trino avec des droits limités |
| Mots de passe par défaut dans `.env.example` (RustFS, Postgres, Airflow, Metabase, OpenMetadata) | Accès trivial si les ports sont exposés | Tous les changer avant le premier `up` : certains ne peuvent plus l'être ensuite (étape 4) |
| *Credential vending* désactivé (RustFS n'a pas de STS) | Trino accède à RustFS avec les clés S3 globales | Accepté tant que RustFS ne propose pas de STS |
| `reset_bronze.sh` recrée une table `frequentation_sncf` obsolète | Table vide et inutile dans `iceberg.bronze` | Retirer ce `CREATE TABLE` du script |
| Extraction dépendante du dossier courant (`Path("data")` dans `main.py`) | Fichiers écrits au mauvais endroit si lancé ailleurs (étape 3) | Construire le chemin à partir de `Path(__file__).parent` |
| Aucune sauvegarde des volumes Docker | Un `docker compose down -v` efface tout | Sauvegarder régulièrement `postgres_data` (`pg_dump`) et le bucket RustFS |

## Environnement cible

La solution doit pouvoir être déployée en local, sur le poste de travail, et sur un VPS cloud. Les sources sont hébergées sur GitHub.

## Historique : prompts d'initialisation

<details>
<summary>Prompt pour le sous-projet d'extraction de données publiques</summary>

> Dans le cadre d'un projet de Data j'aimerai récupérer des données publiques avec Python afin de les ingérer dans la plateforme data et générer un exemple autour de données concernant les traffics ferroviaires de france.
>
> Peux tu me donner un code python pour l'extraction et un jeu de données OpenSource pour mon premier dashboard

</details>

<details>
<summary>Prompt pour l'environnement Docker</summary>

> Peux tu me créer un fichier .yml pour générer l'environnement docker avec les éléments suivant :
>
> - Moteur de calcul : Trino
> - Stockage : RustFS
> - Catalogue Data : Polaris
> - Format de stockage : IceBerg
> - Ingestion Transformation : DBT / Python
> - Orchestration : Airflow
> - Architecture Data : Medaillon (Bronze/Silver/Gold)

</details>

