
import pandas as pd
import pytest

def validate_columns(df, required_columns):
    missing = set(required_columns) - set(df.columns)

    if missing:
        raise ValueError(
            f"Colonnes manquantes : {sorted(missing)}"
        )


def test_validate_columns_success():
    df = pd.DataFrame({
        "nom_gare": ["Paris-Nord"],
        "code_uic_complet": ["87271007"],
    })

    validate_columns(
        df,
        ["nom_gare", "code_uic_complet"],
    )


def test_validate_columns_missing_column():
    df = pd.DataFrame({
        "nom_gare": ["Paris-Nord"],
    })

    with pytest.raises(
        ValueError,
        match="Colonnes manquantes",
    ):
        validate_columns(
            df,
            ["nom_gare", "code_uic_complet"],
        )
