"""
Ingestion des métadonnées du lakehouse dans OpenMetadata
(service openmetadata-ingest du docker-compose).

1. Récupère le jeton JWT du compte de service "ingestion-bot".
2. Ingestion Trino : schémas bronze / silver / gold, tables, colonnes.
3. Ingestion dbt : descriptions, tests et lignage
   bronze -> silver -> gold, à partir du manifest.json de dbt.

Relançable : OpenMetadata met à jour les entités existantes.

Lancement :
    docker compose --profile gouvernance run --rm openmetadata-ingest
"""

import base64
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

OM_URL = os.environ.get("OPENMETADATA_URL", "http://openmetadata:8585/api").rstrip("/")
ADMIN_EMAIL = os.environ.get("OPENMETADATA_ADMIN_EMAIL", "admin@open-metadata.org")
ADMIN_PASSWORD = os.environ.get("OPENMETADATA_ADMIN_PASSWORD", "admin")
SERVICE_NAME = "lakehouse_trino"
TRINO_HOST_PORT = f"{os.environ.get('TRINO_HOST', 'trino')}:{os.environ.get('TRINO_PORT', '8080')}"
TRINO_CATALOG = os.environ.get("TRINO_CATALOG", "iceberg")
SCHEMAS = ["bronze", "silver", "gold"]

# Artefacts dbt : volume alimenté par le DAG Airflow, ou dossier
# target/ d'un "dbt build" lancé à la main. Le plus récent est utilisé.
DBT_ARTIFACT_DIRS = [Path("/dbt-artifacts"), Path("/dbt-target-local")]


def api(method, path, body=None, token=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(OM_URL + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.loads(resp.read() or b"null")
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"{method} {path} -> HTTP {exc.code} : {exc.read().decode(errors='replace')}") from None


def get_ingestion_token():
    for _ in range(60):
        try:
            api("GET", "/v1/system/version")
            break
        except Exception:
            time.sleep(5)
    else:
        sys.exit("OpenMetadata ne répond pas.")

    password_b64 = base64.b64encode(ADMIN_PASSWORD.encode()).decode()
    admin_token = api("POST", "/v1/users/login",
                      {"email": ADMIN_EMAIL, "password": password_b64})["accessToken"]

    bot = api("GET", "/v1/bots/name/ingestion-bot", token=admin_token)
    auth = api("GET", f"/v1/users/auth-mechanism/{bot['botUser']['id']}", token=admin_token)
    return auth["config"]["JWTToken"]


def workflow_config(token):
    return {
        "loggerLevel": "INFO",
        "openMetadataServerConfig": {
            "hostPort": OM_URL,
            "authProvider": "openmetadata",
            "securityConfig": {"jwtToken": token},
        },
    }


def run_workflow(name, config):
    print(f"\n==> Ingestion {name}")
    with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False) as f:
        json.dump(config, f)  # le JSON est du YAML valide
        path = f.name
    result = subprocess.run(["metadata", "ingest", "-c", path])
    os.unlink(path)
    if result.returncode != 0:
        sys.exit(f"Échec de l'ingestion {name} (code {result.returncode}).")


def find_dbt_artifacts():
    candidates = [d for d in DBT_ARTIFACT_DIRS if (d / "manifest.json").is_file()]
    if not candidates:
        return None
    return max(candidates, key=lambda d: (d / "manifest.json").stat().st_mtime)


def main():
    token = get_ingestion_token()
    print("Jeton ingestion-bot récupéré.")

    run_workflow("Trino", {
        "source": {
            "type": "trino",
            "serviceName": SERVICE_NAME,
            "serviceConnection": {"config": {
                "type": "Trino",
                "hostPort": TRINO_HOST_PORT,
                "username": "openmetadata",
                "catalog": TRINO_CATALOG,
            }},
            "sourceConfig": {"config": {
                "type": "DatabaseMetadata",
                "schemaFilterPattern": {"includes": [f"^{s}$" for s in SCHEMAS]},
                "includeViews": True,
                "markDeletedTables": True,
            }},
        },
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": workflow_config(token),
    })

    artifacts = find_dbt_artifacts()
    if artifacts is None:
        print("\nAucun manifest.json dbt trouvé : ingestion dbt ignorée.")
        print("Lance d'abord le DAG Airflow, ou 'dbt build' dans data-platform/dbt/.")
        return

    dbt_source = {"dbtConfigType": "local", "dbtManifestFilePath": str(artifacts / "manifest.json")}
    for key, filename in [("dbtCatalogFilePath", "catalog.json"),
                          ("dbtRunResultsFilePath", "run_results.json")]:
        if (artifacts / filename).is_file():
            dbt_source[key] = str(artifacts / filename)
    print(f"\nArtefacts dbt utilisés : {artifacts}")

    run_workflow("dbt", {
        "source": {
            "type": "dbt",
            "serviceName": SERVICE_NAME,
            "sourceConfig": {"config": {
                "type": "DBT",
                "dbtConfigSource": dbt_source,
                "dbtUpdateDescriptions": True,
                "includeTags": True,
            }},
        },
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": workflow_config(token),
    })

    print("\nIngestion terminée : http://localhost:8585")


if __name__ == "__main__":
    main()
