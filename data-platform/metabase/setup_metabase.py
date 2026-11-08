"""
Initialisation de Metabase (service metabase-setup du docker-compose).

1. Au premier lancement : crée le compte administrateur (assistant
   d'installation de Metabase).
2. Ajoute la connexion "Lakehouse (Trino)" vers le schéma gold, si elle
   n'existe pas encore.

Relançable sans risque : rien n'est recréé si c'est déjà fait.
Utilise uniquement la bibliothèque standard Python.
"""

import json
import os
import sys
import time
import urllib.error
import urllib.request

URL = os.environ.get("METABASE_URL", "http://metabase:3000").rstrip("/")
EMAIL = os.environ["METABASE_ADMIN_EMAIL"]
PASSWORD = os.environ["METABASE_ADMIN_PASSWORD"]
DB_NAME = "Lakehouse (Trino)"
DB_DETAILS = {
    "host": os.environ.get("TRINO_HOST", "trino"),
    "port": int(os.environ.get("TRINO_PORT", "8080")),
    "catalog": os.environ.get("TRINO_CATALOG", "iceberg"),
    "schema": os.environ.get("TRINO_SCHEMA", "gold"),
    "user": "metabase",
    "ssl": False,
}


def api(method, path, body=None, session=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(URL + path, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    if session:
        req.add_header("X-Metabase-Session", session)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read()
            return json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        detail = exc.read().decode(errors="replace")
        raise RuntimeError(f"{method} {path} -> HTTP {exc.code} : {detail}") from None


def main():
    for _ in range(60):
        try:
            if api("GET", "/api/health").get("status") == "ok":
                break
        except Exception:
            pass
        time.sleep(5)
    else:
        sys.exit("Metabase ne répond pas.")

    props = api("GET", "/api/session/properties")

    if not props.get("has-user-setup"):
        print(f"Création du compte administrateur {EMAIL}")
        api("POST", "/api/setup", {
            "token": props["setup-token"],
            "user": {
                "first_name": "Admin",
                "last_name": "DataLakeHouse",
                "email": EMAIL,
                "password": PASSWORD,
            },
            "prefs": {
                "site_name": "DataLakeHouse",
                "site_locale": "fr",
                "allow_tracking": False,
            },
        })
    else:
        print("Compte administrateur déjà présent.")

    session = api("POST", "/api/session", {"username": EMAIL, "password": PASSWORD})["id"]

    databases = api("GET", "/api/database", session=session)
    databases = databases.get("data", databases) if isinstance(databases, dict) else databases
    if any(db.get("name") == DB_NAME for db in databases):
        print(f"Connexion « {DB_NAME} » déjà présente.")
    else:
        print(f"Création de la connexion « {DB_NAME} » -> {DB_DETAILS['catalog']}.{DB_DETAILS['schema']}")
        api("POST", "/api/database", {
            "engine": "starburst",   # pilote Metabase compatible Trino
            "name": DB_NAME,
            "details": DB_DETAILS,
            "is_full_sync": True,
        }, session=session)

    print("Metabase prêt : http://localhost:3000")


if __name__ == "__main__":
    main()
