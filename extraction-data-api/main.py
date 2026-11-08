
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd
import re
import requests

# --------------------------------------------------
# CONFIGURATION
# --------------------------------------------------

BASE_URL = (
    "https://data.sncf.com/api/explore/v2.1/catalog/"
    "datasets/frequentation-gares/exports/csv"
)

OUTPUT_DIR = Path("data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

RAW_FILE = OUTPUT_DIR / "frequentation_gares_raw.csv"
CLEAN_FILE = OUTPUT_DIR / "frequentation_gares_clean.csv"
PARQUET_FILE = OUTPUT_DIR / "frequentation_gares_clean.parquet"

# --------------------------------------------------
# 1. EXTRACT : téléchargement du CSV public
# --------------------------------------------------

def extract_data():
    print("Extraction des données SNCF...")

    response = requests.get(
        BASE_URL,
        params={"lang": "fr"},
        timeout=120,
    )
    response.raise_for_status()

    if not response.content:
        raise ValueError("Le fichier téléchargé est vide.")

    RAW_FILE.write_bytes(response.content)

    print(f"Fichier brut enregistré : {RAW_FILE}")
    return response.content


# --------------------------------------------------
# 2. TRANSFORM : nettoyage et normalisation
# --------------------------------------------------

def transform_data(content):
    from io import BytesIO

    # Le portail peut fournir des caractères accentués.
    # Tout est lu en texte : les identifiants (code postal, code UIC)
    # ne doivent pas être convertis en nombres. Les colonnes de
    # fréquentation sont converties explicitement plus bas.
    df = pd.read_csv(
        BytesIO(content), sep=";", encoding="utf-8-sig", dtype=str
    )

    # Normaliser les noms des colonnes.
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
    )

    print("Colonnes reçues :", df.columns.tolist())

    # Identifier les colonnes de fréquentation annuelle.
    # Exemple : "Total Voyageurs 2024"


    year_columns = [
        col for col in df.columns
        if re.search(r"(?:total_?voyageurs_?)(\d{4})$", col)
        and "non_voyageurs" not in col
    ]

    if not year_columns:
        raise ValueError(
            "Aucune colonne annuelle trouvée. "
            f"Colonnes disponibles : {df.columns.tolist()}"
        )

    print("Colonnes annuelles détectées :", year_columns)

    # Les noms des colonnes descriptives peuvent évoluer :
    # l'export actuel est en snake_case (ex. "code_uic_complet"),
    # les anciens exports utilisaient des libellés (ex. "code uic").
    rename_map = {
        "nom de la gare": "nom_gare",
        "code uic": "code_uic",
        "code_uic_complet": "code_uic",
        "code postal": "code_postal",
        "direction régionale gares": "direction_regionale",
        "direction_regionale_gares": "direction_regionale",
        "segmentation drg": "segmentation_drg",
        "segmentation marketing": "segmentation_marketing",
    }
    df = df.rename(columns=rename_map)

    # La clé de déduplication (code_uic, annee) est indispensable :
    # sans code_uic, on ne garderait qu'une ligne par année.
    if "code_uic" not in df.columns:
        raise ValueError(
            "Colonne code UIC introuvable. "
            f"Colonnes disponibles : {df.columns.tolist()}"
        )

    # La source fournit certains codes postaux sans le zéro initial
    # (ex. "1500" pour 01500) : on les complète à 5 chiffres.
    if "code_postal" in df.columns:
        df["code_postal"] = (
            df["code_postal"]
            .str.strip()
            .str.replace(r"\.0$", "", regex=True)
            .str.zfill(5)
        )

    # Ne garder que les colonnes utiles présentes dans le fichier.
    dimensions = [
        col for col in [
            "nom_gare",
            "code_uic",
            "code_postal",
            "direction_regionale",
            "segmentation_drg",
            "segmentation_marketing",
        ]
        if col in df.columns
    ]

    # Convertir les colonnes annuelles en lignes.
    long_df = df.melt(
        id_vars=dimensions,
        value_vars=year_columns,
        var_name="indicateur_annee",
        value_name="voyageurs",
    )

    long_df["annee"] = (
        long_df["indicateur_annee"]
        .str.extract(r"(\d{4})$")
        .astype("Int64")
    )

    # Convertir les nombres, y compris les espaces
    # et les séparateurs décimaux français.
    long_df["voyageurs"] = pd.to_numeric(
        long_df["voyageurs"]
        .astype(str)
        .str.replace("\u202f", "", regex=False)
        .str.replace("\xa0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False),
        errors="coerce",
    )

    long_df = long_df.dropna(
        subset=["annee", "voyageurs"]
    )

    long_df["annee"] = long_df["annee"].astype(int)
    long_df["date_extraction_utc"] = datetime.now(
        timezone.utc
    ).isoformat()

    # Contrôles de qualité simples.
    if "nom_gare" in long_df.columns:
        long_df = long_df[
            long_df["nom_gare"].notna()
            & long_df["nom_gare"].astype(str).str.strip().ne("")
        ]

    if (long_df["voyageurs"] < 0).any():
        raise ValueError("Des valeurs négatives ont été détectées.")

    long_df = long_df.drop_duplicates(subset=["code_uic", "annee"])

    return long_df


# --------------------------------------------------
# 3. LOAD : écriture des données préparées
# --------------------------------------------------

def load_data(df):
    df.to_csv(
        CLEAN_FILE,
        index=False,
        encoding="utf-8-sig",
    )

    df.to_parquet(
        PARQUET_FILE,
        index=False,
    )

    print(f"Données CSV : {CLEAN_FILE}")
    print(f"Données Parquet : {PARQUET_FILE}")
    print(f"Nombre de lignes : {len(df)}")


# --------------------------------------------------
# EXÉCUTION DU PIPELINE
# --------------------------------------------------

if __name__ == "__main__":
    raw_content = extract_data()
    clean_df = transform_data(raw_content)
    load_data(clean_df)

    print("\nAperçu des données :")
    print(clean_df.head(10).to_string(index=False))

    print("\nPipeline terminé.")

