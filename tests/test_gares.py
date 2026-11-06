import importlib.util
from pathlib import Path

import pytest

# extraction-data-api n'est pas un package Python (tiret dans le nom) :
# on charge le module à partir de son chemin.
_spec = importlib.util.spec_from_file_location(
    "gares", Path(__file__).resolve().parents[1] / "extraction-data-api" / "gares.py"
)
gares = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gares)

EN_TETE = "﻿nom;libellecourt;segment_drg;position_geographique;codeinsee;codes_uic;id\n"
LIGNE = "Abbeville;ABB;B;50.10221, 1.82449;80001;87317362;c4a3e70f\n"


def export(nb_lignes, ligne=LIGNE):
    return (EN_TETE + ligne * nb_lignes).encode("utf-8")


def test_parse_position_latitude_en_premier():
    assert gares.parse_position("50.10221, 1.82449") == (50.10221, 1.82449)


@pytest.mark.parametrize("position", ["", None, "50.1", "abc, def"])
def test_parse_position_invalide(position):
    assert gares.parse_position(position) is None


def test_valider_export_ok():
    assert gares.valider_export(export(gares.NB_GARES_MINIMUM)) == gares.NB_GARES_MINIMUM


def test_valider_export_valeurs_multiples_entre_guillemets():
    ligne = 'Ajaccio;AJA;"A;B";41.9269, 8.7344;2A004;"87271494;87001479";x\n'
    assert gares.valider_export(export(gares.NB_GARES_MINIMUM, ligne)) == gares.NB_GARES_MINIMUM


def test_valider_export_colonne_manquante():
    contenu = ("nom;libellecourt\n" + "Abbeville;ABB\n" * gares.NB_GARES_MINIMUM).encode()
    with pytest.raises(ValueError, match="Colonnes manquantes"):
        gares.valider_export(contenu)


def test_valider_export_tronque():
    with pytest.raises(ValueError, match="incomplet"):
        gares.valider_export(export(10))


def test_valider_export_positions_absentes():
    ligne = "Abbeville;ABB;B;;80001;87317362;c4a3e70f\n"
    with pytest.raises(ValueError, match="sans position"):
        gares.valider_export(export(gares.NB_GARES_MINIMUM, ligne))
