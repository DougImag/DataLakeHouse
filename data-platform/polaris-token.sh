#!/usr/bin/env bash

POLARIS_URL="http://localhost:8181"

TOKEN_RESPONSE=$(curl -sS -X POST \
  "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
  -u "root:s3cr3t" \
  -H "Polaris-Realm: POLARIS" \
  -d "grant_type=client_credentials" \
  -d "scope=PRINCIPAL_ROLE:ALL")

TOKEN=$(printf '%s' "$TOKEN_RESPONSE" |
  python -c "import sys,json; print(json.load(sys.stdin).get('access_token',''))" 2>/dev/null)

if [ -z "$TOKEN" ]; then
  echo "ERREUR : impossible de recuperer le jeton."
  echo "Reponse de Polaris :"
  printf '%s\n' "$TOKEN_RESPONSE"
  return 1 2>/dev/null || exit 1
fi

export TOKEN
echo "Jeton Polaris recupere avec succes."
echo "Le jeton est disponible dans la variable TOKEN."
# Image Airflow du projet : Airflow officiel + un environnement Python
# séparé pour le code du projet (extraction, ingestion Bronze, dbt).
#
# Le venv séparé évite tout conflit entre les dépendances d'Airflow et
# celles du projet (pandas 3, dbt-core...). Les tâches du DAG appellent
# directement /opt/dlh-venv/bin/python et /opt/dlh-venv/bin/dbt.
#
# Contexte de build : la racine du dépôt (voir docker-compose.yml).
