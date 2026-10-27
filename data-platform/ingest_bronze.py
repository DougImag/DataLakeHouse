#!/usr/bin/env python3
"""
Ingestion Bronze : CSV -> Iceberg via Trino.
Toutes les colonnes du CSV sont conservées en VARCHAR. Deux colonnes techniques
sont ajoutées : source_fichier et date_ingestion.
Aucune suppression de table n'est effectuée automatiquement.
"""
import argparse
import csv
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import trino

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "ingest")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "bronze")
DEFAULT_TABLE = os.getenv("BRONZE_TABLE", "frequentation_sncf_v2")
TECH_COLS = ("source_fichier", "date_ingestion")


def find_default_csv():
    here = Path(__file__).resolve()
    candidates = [
        here.parent / "extraction-data-api/data/frequentation_gares_raw.csv",
        here.parent.parent / "extraction-data-api/data/frequentation_gares_raw.csv",
        here.parent.parent.parent / "extraction-data-api/data/frequentation_gares_raw.csv",
        here.parent.parent / "DataLakeHouse/extraction-data-api/data/frequentation_gares_raw.csv",
        Path.cwd() / "extraction-data-api/data/frequentation_gares_raw.csv",
        Path.cwd() / "DataLakeHouse/extraction-data-api/data/frequentation_gares_raw.csv",
    ]
    for candidate in candidates:
        if candidate.is_file():
            return candidate.resolve()
    return candidates[0].resolve()


def open_csv(path):
    for encoding in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            with path.open("r", encoding=encoding, newline="") as f:
                sample = f.read(16384)
            try:
                dialect = csv.Sniffer().sniff(sample, delimiters=",;\t|")
            except csv.Error:
                dialect = csv.excel
                dialect.delimiter = ";"
            return path.open("r", encoding=encoding, newline=""), dialect, encoding
        except UnicodeDecodeError:
            continue
    raise RuntimeError("Encodage du CSV non reconnu.")


def ident(name):
    return '"' + name.replace('"', '""') + '"'


def sql_string(value):
    if value is None:
        return "NULL"
    return "'" + str(value).replace("'", "''") + "'"


def fetch_all(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()


def qualified_table(catalog, schema, table):
    return f"{ident(catalog)}.{ident(schema)}.{ident(table)}"


def table_exists(cursor, catalog, schema, table):
    rows = fetch_all(
        cursor,
        f"SELECT 1 FROM {ident(catalog)}.information_schema.tables "
        f"WHERE table_schema = {sql_string(schema)} "
        f"AND table_name = {sql_string(table)}",
    )
    return bool(rows)


def get_columns(cursor, fqtn):
    return [str(row[0]).lower() for row in fetch_all(cursor, f"DESCRIBE {fqtn}")]


def create_table(cursor, fqtn, headers):
    columns = [f"{ident(h)} VARCHAR" for h in headers]
    columns += [
        f"{ident('source_fichier')} VARCHAR",
        f"{ident('date_ingestion')} TIMESTAMP(6) WITH TIME ZONE",
    ]
    sql = (
        f"CREATE TABLE {fqtn} (\n  " + ",\n  ".join(columns)
        + "\n) WITH (format = 'PARQUET', format_version = 2)"
    )
    fetch_all(cursor, sql)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", nargs="?", default=str(find_default_csv()))
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--no-replace-source", action="store_true")
    args = parser.parse_args()

    if args.batch_size < 1:
        sys.exit("Erreur : --batch-size doit être supérieur à zéro.")

    path = Path(args.csv_path).expanduser().resolve()
    if not path.is_file():
        sys.exit(f"Fichier introuvable : {path}")

    # Lire les en-têtes avant de se connecter à Trino.
    stream, dialect, encoding = open_csv(path)
    try:
        reader = csv.reader(stream, dialect=dialect)
        try:
            headers = [h.strip() for h in next(reader)]
        except StopIteration:
            sys.exit("CSV vide ou sans en-têtes.")
    finally:
        stream.close()

    if not headers or any(not h for h in headers):
        sys.exit("Le CSV contient un en-tête vide.")
    lowered = [h.lower() for h in headers]
    if len(set(lowered)) != len(lowered):
        sys.exit(f"En-têtes en double (casse ignorée) : {headers}")
    if any(h in TECH_COLS for h in lowered):
        sys.exit(f"Le CSV utilise un nom de colonne réservé : {TECH_COLS}")

    source = path.name
    fqtn = qualified_table(TRINO_CATALOG, TRINO_SCHEMA, args.table)
    print(f"Fichier : {path} (encodage {encoding}, séparateur {dialect.delimiter!r}, {len(headers)} colonnes)")
    print(f"Table cible : {TRINO_CATALOG}.{TRINO_SCHEMA}.{args.table}")

    conn = None
    try:
        conn = trino.dbapi.connect(
            host=TRINO_HOST, port=TRINO_PORT, user=TRINO_USER,
            catalog=TRINO_CATALOG, schema=TRINO_SCHEMA, http_scheme="http",
        )
        cur = conn.cursor()

        if not table_exists(cur, TRINO_CATALOG, TRINO_SCHEMA, args.table):
            print("Table absente : création à partir des en-têtes du CSV.")
            create_table(cur, fqtn, headers)
        else:
            actual = get_columns(cur, fqtn)
            expected = lowered + list(TECH_COLS)
            if actual != expected:
                sys.exit(
                    "Le schéma de la table ne correspond pas au CSV.\n"
                    f"Table : {actual}\nAttendu : {expected}\n"
                    "Aucune table n'a été supprimée. Relance avec un nom de table neuf, par exemple :\n"
                    f"python ingest_bronze.py --table {args.table}_v3"
                )

        # Relecture CSV depuis le début, en gardant le dialecte détecté.
        stream, dialect, _ = open_csv(path)
        total_read = 0
        total_inserted = 0
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")
        ts_sql = f"TIMESTAMP '{ts} UTC'"
        insert_cols = ", ".join([ident(h) for h in headers] + [
            ident("source_fichier"), ident("date_ingestion")
        ])
        insert_prefix = f"INSERT INTO {fqtn} ({insert_cols}) VALUES "

        try:
            reader = csv.DictReader(stream, dialect=dialect)
            reader.fieldnames = [h.strip() for h in (reader.fieldnames or [])]

            if not args.no_replace_source:
                print(f"Remplacement des lignes existantes pour {source}.")
                fetch_all(
                    cur,
                    f"DELETE FROM {fqtn} WHERE {ident('source_fichier')} = {sql_string(source)}",
                )

            batch = []

            def flush():
                nonlocal total_inserted, batch
                if batch:
                    fetch_all(cur, insert_prefix + ",\n".join(batch))
                    total_inserted += len(batch)
                    print(f"  {total_inserted} lignes insérées")
                    batch = []

            for row in reader:
                values = [sql_string(row.get(h)) for h in headers]
                values.extend([sql_string(source), ts_sql])
                batch.append("(" + ", ".join(values) + ")")
                total_read += 1
                if len(batch) >= args.batch_size:
                    flush()
            flush()
        finally:
            stream.close()

        rows = fetch_all(
            cur,
            f"SELECT count(*) FROM {fqtn} WHERE {ident('source_fichier')} = {sql_string(source)}",
        )
        count = rows[0][0] if rows else 0
        print(f"Terminé : {total_read} lignes lues, {total_inserted} insérées, {count} présentes en table.")
        if not args.no_replace_source and count != total_read:
            sys.exit("ATTENTION : le nombre de lignes en table ne correspond pas au CSV.")

    except Exception as exc:
        print(f"ÉCHEC DE L'INGESTION : {exc}", file=sys.stderr)
        print("Aucune suppression de table n'a été tentée. Une erreur Iceberg/Polaris/RustFS nécessite une correction de configuration ou de droits.", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn is not None:
            conn.close()


if __name__ == "__main__":
    main()
