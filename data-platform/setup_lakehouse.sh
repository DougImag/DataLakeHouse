#!/usr/bin/env bash
# Setup Polaris (catalog "lakehouse") + bucket RustFS + schema bronze via Trino
set -euo pipefail

# ----------------------- À ADAPTER -----------------------
POLARIS_URL="${POLARIS_URL:-http://localhost:8181}"
CLIENT_ID="${CLIENT_ID:-root}"
CLIENT_SECRET="${CLIENT_SECRET:-s3cr3t}"
CATALOG="${CATALOG:-lakehouse}"
BUCKET="${BUCKET:-lakehouse}"

RUSTFS_INTERNAL="http://rustfs:9000"        # vu depuis Polaris/Trino (réseau Docker)
RUSTFS_ACCESS_KEY="${RUSTFS_ACCESS_KEY:?exporte RUSTFS_ACCESS_KEY}"
RUSTFS_SECRET_KEY="${RUSTFS_SECRET_KEY:?exporte RUSTFS_SECRET_KEY}"
DOCKER_NETWORK="${DOCKER_NETWORK:?exporte DOCKER_NETWORK (docker network ls)}"

TRINO_CONTAINER="${TRINO_CONTAINER:-dlh-trino}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TRINO_PROPS="${TRINO_PROPS:-$SCRIPT_DIR/trino/catalog/iceberg.properties}"   # chemin local du fichier
# ---------------------------------------------------------

# Remplace (ou ajoute) une ligne "cle=valeur" dans un fichier .properties.
# Portable GNU sed (Linux, Git Bash) et BSD sed (macOS) : pas de "sed -i",
# dont la syntaxe diffère. Le contenu est réécrit dans le même fichier
# (cat >), ce qui conserve ses droits.
set_property() {
  local file="$1" key="$2" value="$3" tmp
  tmp="$(mktemp)"
  if grep -q "^${key}=" "$file"; then
    sed "s|^${key}=.*|${key}=${value}|" "$file" > "$tmp"
  else
    cat "$file" > "$tmp"
    [ -z "$(tail -c1 "$file")" ] || echo >> "$tmp"   # fin de ligne manquante
    echo "${key}=${value}" >> "$tmp"
  fi
  cat "$tmp" > "$file"
  rm -f "$tmp"
}

API="$POLARIS_URL/api/management/v1"

echo "==> 1. Token OAuth2"
TOKEN=$(curl -sf "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials \
  -d client_id="$CLIENT_ID" \
  -d client_secret="$CLIENT_SECRET" \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
[ -n "$TOKEN" ] && [ "$TOKEN" != "null" ] || { echo "Token invalide"; exit 1; }
AUTH=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")

echo "==> 2. Bucket '$BUCKET' dans RustFS"
docker run --rm --network "$DOCKER_NETWORK" \
  -e AWS_ACCESS_KEY_ID="$RUSTFS_ACCESS_KEY" \
  -e AWS_SECRET_ACCESS_KEY="$RUSTFS_SECRET_KEY" \
  -e AWS_DEFAULT_REGION=us-east-1 \
  amazon/aws-cli --endpoint-url "$RUSTFS_INTERNAL" s3 mb "s3://$BUCKET" 2>&1 \
  | grep -v "BucketAlreadyOwnedByYou" || true

echo "==> 3. Catalogs existants"
curl -sf "$API/catalogs" "${AUTH[@]}" | jq -r '.catalogs[].name' || true

echo "==> 4. Création du catalog '$CATALOG' (si absent)"
if curl -sf "$API/catalogs/$CATALOG" "${AUTH[@]}" >/dev/null 2>&1; then
  echo "    déjà présent"
else
  curl -sf -X POST "$API/catalogs" "${AUTH[@]}" -d @- <<EOF
{
  "catalog": {
    "name": "$CATALOG",
    "type": "INTERNAL",
    "readOnly": false,
    "properties": { "default-base-location": "s3://$BUCKET/" },
    "storageConfigInfo": {
      "storageType": "S3",
      "allowedLocations": ["s3://$BUCKET/"],
      "endpoint": "$RUSTFS_INTERNAL",
      "endpointInternal": "$RUSTFS_INTERNAL",
      "pathStyleAccess": true,
      "stsUnavailable": true
    }
  }
}
EOF
  echo "    créé"
fi

echo "==> 5. Droits : catalog role catalog_admin -> principal role service_admin"
curl -s -X POST "$API/catalogs/$CATALOG/catalog-roles" "${AUTH[@]}" \
  -d '{"catalogRole":{"name":"catalog_admin"}}' >/dev/null || true
curl -sf -X PUT "$API/catalogs/$CATALOG/catalog-roles/catalog_admin/grants" "${AUTH[@]}" \
  -d '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}'
curl -sf -X PUT "$API/principal-roles/service_admin/catalog-roles/$CATALOG" "${AUTH[@]}" \
  -d '{"catalogRole":{"name":"catalog_admin"}}'

echo "==> 6. Trino : désactiver le vending de credentials (RustFS n'a pas de STS)"
if [ -f "$TRINO_PROPS" ]; then
  set_property "$TRINO_PROPS" "iceberg.rest-catalog.vended-credentials-enabled" "false"
  echo "    $TRINO_PROPS : $(grep '^iceberg.rest-catalog.vended-credentials-enabled=' "$TRINO_PROPS")"
else
  echo "    $TRINO_PROPS introuvable : modifie la ligne à la main"
fi
docker restart "$TRINO_CONTAINER" >/dev/null

echo "==> Attente de Trino..."
until docker exec "$TRINO_CONTAINER" trino --execute "SELECT 1" >/dev/null 2>&1; do sleep 3; done

echo "==> 7. Test + bronze layer"
docker exec "$TRINO_CONTAINER" trino --execute "SHOW SCHEMAS FROM iceberg"
docker exec "$TRINO_CONTAINER" trino --execute \
  "CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location = 's3://$BUCKET/bronze/')"
docker exec "$TRINO_CONTAINER" trino --execute "SHOW SCHEMAS FROM iceberg"

echo "Terminé."
