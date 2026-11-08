# Projet dbt `sncf_lakehouse`

Transforme la couche **Bronze** (données brutes, tout en `VARCHAR`) en couches **Silver** (données typées et normalisées) et **Gold** (marts prêts pour le dashboard), sur Trino et Iceberg (catalogue Polaris).

## Lignage

```
bronze.frequentation_sncf_v2                     (source, chargée par ingest_bronze.py)
        │
        ▼
int_bronze_derniere_ingestion                    (éphémère : dernière ingestion par gare)
        │
        │   bronze.gares_voyageurs               (source : référentiel des gares, position GPS)
        │           │
        │           ▼
        │   silver.slv_gares_geolocalisation     (1 ligne / code UIC : latitude, longitude)
        │           │  (jointure sur code_uic)
        ├──> silver.slv_gares ◄┘                 (1 ligne / gare, avec latitude / longitude)
        └──> silver.slv_frequentation_annuelle   (1 ligne / gare × année)
                        │
                        ▼
              gold.gld_frequentation_gares       (gare × année + évolution, rang, part)
                        │
                        ├──> gold.gld_frequentation_regions   (région × année)
                        └──> gold.gld_kpi_national            (année)
```

## Lancer

Depuis ce dossier, avec le venv du projet activé et la stack démarrée (Bronze chargée) :

```bash
pip install -r requirements.txt      # dbt-core + dbt-trino (une seule fois)
export DBT_PROFILES_DIR=$PWD
dbt debug
dbt build
```

La connexion est définie dans `profiles.yml`. Elle peut être modifiée par des variables d'environnement :

| Variable | Défaut |
|---|---|
| `TRINO_HOST` | `localhost` |
| `TRINO_PORT` | `8080` |
| `DBT_TRINO_USER` | `dbt` |
| `TRINO_CATALOG` | `iceberg` |
| `DBT_THREADS` | `4` |

## Règles de transformation

### Silver

- **Déduplication** : si plusieurs fichiers ont été ingérés en Bronze, seule la ligne de l'ingestion la plus récente est gardée pour chaque gare (`int_bronze_derniere_ingestion`).
- **Texte** : espaces retirés, chaînes vides remplacées par `NULL` (macro `texte_ou_null`).
- **Nombres** : conversion tolérante (espaces, virgule décimale), `NULL` si la valeur est invalide (macros `vers_double` et `vers_bigint`).
- **Code postal** : complété à 5 chiffres (`1500` devient `01500`). Le département en est déduit (`2A`/`2B` pour la Corse, 3 chiffres pour l'outre-mer).
- **Années en lignes** : les colonnes `total_voyageurs_AAAA` sont détectées **automatiquement** dans le schéma Bronze, y compris la variante `totalvoyageurs2017` de la source. Quand la SNCF publiera une nouvelle année, elle sera prise en compte sans modifier le modèle.

### Gold

- Les dimensions non renseignées valent `Non renseignée`, pour que les filtres du dashboard restent lisibles.
- `evolution_pct` n'est calculé que si l'année N-1 existe et n'est pas nulle. Sinon il vaut `NULL`, par exemple pour une gare ouverte récemment.
- `gld_kpi_national.evolution_vs_reference_pct` compare chaque année à 2019 par défaut. Pour changer d'année : `dbt build --vars '{annee_reference: 2023}'`.

## Tests

`dbt build` lance 45 tests. Si un test échoue, les modèles qui en dépendent ne sont pas construits.

| Test | Où |
|---|---|
| `not_null`, `unique`, `accepted_values`, `relationships` | tests standards de dbt |
| `combinaison_unique` | unicité de (`code_uic`, `annee`), de (`direction_regionale`, `annee`)… |
| `positif_ou_nul` | nombres de voyageurs, taux |
| `format_regex` | code UIC sur 8 chiffres, code postal sur 5 chiffres |
| `compris_entre` | latitude / longitude : plage France métropolitaine dans `slv_gares_geolocalisation` (détecte une inversion lat/lon), plage WGS84 dans `slv_gares` |

Les tests génériques du projet sont définis dans `tests/generic/tests_generiques.sql`.

## Conventions

- Préfixes : `int_` (intermédiaire, éphémère), `slv_` (Silver), `gld_` (Gold).
- Schémas : `silver` et `gold`, utilisés tels quels grâce à la macro `generate_schema_name`.
- Matérialisation : tables Iceberg au format Parquet, remplacées avec `CREATE OR REPLACE TABLE`. Chaque run crée un nouveau snapshot Iceberg, et les versions précédentes restent consultables :

  ```sql
  SELECT * FROM iceberg.gold."gld_kpi_national$snapshots";
  SELECT * FROM iceberg.gold.gld_kpi_national FOR VERSION AS OF <snapshot_id>;
  ```

- Documentation et lignage interactif : `dbt docs generate && dbt docs serve --port 8085`.
