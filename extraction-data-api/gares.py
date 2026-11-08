"""
Extraction du référentiel des gares de voyageurs SNCF (avec leur position GPS).

Source : https://ressources.data.sncf.com/explore/dataset/gares-de-voyageurs/
(SNCF Gares & Connexions, licence ODbL, mise à jour quotidienne).

Le fichier est enregistré tel quel dans data/gares_voyageurs_raw.csv
(séparateur ";"), puis chargé en Bronze par data-platform/ingest_bronze.py.
Le découpage de la position en latitude / longitude est fait dans dbt
(modèle Silver slv_gares_geolocalisation).

Particularités de l'export :
  - position_geographique = "<latitude>, <longitude>" (latitude EN PREMIER) ;
  - codes_uic et segment_drg peuvent contenir plusieurs valeurs séparées par ";"
    (ces cellules sont alors entre guillemets).

Usage (depuis n'importe quel dossier) :
    python extraction-data-api/gares.py
"""

import csv
import io
from pathlib import Path

EXPORT_URL = (
    "https://ressources.data.sncf.com/api/explore/v2.1/catalog/"
    "datasets/gares-de-voyageurs/exports/csv"
)

# Chemin construit à partir du script : le fichier arrive toujours dans
# extraction-data-api/data/, quel que soit le dossier courant.
OUTPUT_DIR = Path(__file__).resolve().parent / "data"
RAW_FILE = OUTPUT_DIR / "gares_voyageurs_raw.csv"

COLONNES_ATTENDUES = [
    "nom",
    "libellecourt",
    "segment_drg",
    "position_geographique",
    "codeinsee",
    "codes_uic",
    "id",
]

NB_GARES_MINIMUM = 2000  # garde-fou contre un export tronqué (~2 800 gares)


def parse_position(position):
    """'48.85, 2.35' -> (48.85, 2.35). None si la position est vide ou invalide."""
    if not position or "," not in position:
        return None
    lat, lon = (p.strip() for p in position.split(",", 1))
    try:
        return float(lat), float(lon)
    except ValueError:
        return None


def valider_export(content):
    """Vérifie les colonnes, le nombre de gares et les positions de l'export.

    Renvoie le nombre de gares. Lève ValueError si l'export est inutilisable.
    """
    texte = content.decode("utf-8-sig")
    reader = csv.DictReader(io.StringIO(texte), delimiter=";")
    colonnes = [c.strip().lower() for c in (reader.fieldnames or [])]

    manquantes = sorted(set(COLONNES_ATTENDUES) - set(colonnes))
    if manquantes:
        raise ValueError(
            f"Colonnes manquantes dans l'export des gares : {manquantes}. "
            f"Colonnes reçues : {colonnes}"
        )

    lignes = list(reader)
    if len(lignes) < NB_GARES_MINIMUM:
        raise ValueError(
            f"Export des gares incomplet : {len(lignes)} lignes "
            f"(minimum attendu : {NB_GARES_MINIMUM})."
        )

    sans_position = [
        r["nom"] for r in lignes if parse_position(r["position_geographique"]) is None
    ]
    if len(sans_position) > len(lignes) * 0.05:
        raise ValueError(
            f"{len(sans_position)} gares sans position exploitable "
            f"(ex. : {sans_position[:5]})."
        )

    return len(lignes)


def extract_gares():
    import requests  # import local : les tests n'ont pas besoin de requests

    print("Extraction du référentiel des gares de voyageurs...")
    response = requests.get(
        EXPORT_URL,
        params={"delimiter": ";", "lang": "fr"},
        timeout=120,
    )
    response.raise_for_status()

    nb_gares = valider_export(response.content)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    RAW_FILE.write_bytes(response.content)

    print(f"Fichier brut enregistré : {RAW_FILE}")
    print(f"Nombre de gares : {nb_gares}")
    return RAW_FILE


if __name__ == "__main__":
    extract_gares()
