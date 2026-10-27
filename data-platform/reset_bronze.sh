#!/usr/bin/env bash
# Réinitialise la bronze layer : drop des tables, nettoyage du bucket, recréation propre en Parquet
set -euo pipefail

BUCKET="${BUCKET:-lakehouse}"
TRINO_CONTAINER="${TRINO_CONTAINER:-trino}"
RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:?exporte RUSTFS_ACCESS_KEY}"
RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:?exporte RUSTFS_SECRET_KEY}"
DOCKER_NETWORK="${DOCKER_NETWORK:?exporte DOCKER_NETWORK}"

trino() {
  docker exec "$TRINO_CONTAINER" trino --output-format CSV_UNQUOTED --execute "$1" 2> >(grep -v -E "jline|dumb terminal" >&2) | tr -d '\r'
}

echo "==> 1. Drop de toutes les tables du schéma bronze"
for t in $(trino "SHOW TABLES FROM iceberg.bronze"); do
  echo "    DROP $t"
  trino "DROP TABLE IF EXISTS iceberg.bronze.\"$t\""
done

echo "==> 2. Drop du schéma bronze"
trino "DROP SCHEMA IF EXISTS iceberg.bronze"

echo "==> 3. Nettoyage du préfixe s3://$BUCKET/bronze/ (fichiers orphelins)"
docker run --rm --network "$DOCKER_NETWORK" \
  -e AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" \
  -e AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
  -e AWS_DEFAULT_REGION=us-east-1 \
  amazon/aws-cli --endpoint-url http://rustfs:9000 \
  s3 rm "s3://$BUCKET/bronze/" --recursive || true

echo "==> 4. Recréation du schéma et des tables (Iceberg v2 / Parquet)"
trino "CREATE SCHEMA iceberg.bronze WITH (location = 's3://$BUCKET/bronze/')"

# Bronze = tout en varchar (donnée brute, aucune transformation) + colonnes techniques
trino "
CREATE TABLE iceberg.bronze.frequentation_sncf (
  gare_code       varchar,
  gare_nom        varchar,
  date_mesure     varchar,
  frequentation   varchar,
  source_fichier  varchar,
  date_ingestion  timestamp(6) with time zone
)
WITH (
  format = 'PARQUET',
  format_version = 2
)"

# >>> Ajoute ici les autres tables bronze sur le même modèle <<<

echo "==> 5. Vérification"
trino "SHOW TABLES FROM iceberg.bronze"
trino "SELECT 'frequentation_sncf' AS table_name, count(*) AS nb_lignes FROM iceberg.bronze.frequentation_sncf"
echo "Terminé."