# SNCF Lakehouse starter (Docker Compose)

Local development scaffold for:
- Trino (SQL query engine)
- MinIO (S3-compatible object storage)
- Apache Polaris (Iceberg REST catalog)
- Apache Iceberg (table format; **not a separate container**)
- Python / dbt (ingestion and transformations)
- Apache Airflow (orchestration)
- Medallion layers: Bronze / Silver / Gold

## Start

1. Install Docker Desktop with Docker Compose v2.
2. Copy `.env.example` to `.env`.
3. From this folder run:

   ```bash
   docker compose up -d
   docker compose ps
   ```

4. Open:
   - MinIO Console: http://localhost:9001
   - Trino UI: http://localhost:8080
   - Airflow UI: http://localhost:8081 (user/password from `.env`, defaults admin/admin)
   - Polaris API: http://localhost:8181

## Important setup notes

This is a **development scaffold**, not a production-secure deployment.
Polaris bootstrap variables and REST/OAuth settings can vary by Polaris image/version. Verify the Polaris startup logs and create a catalog named `sncf` with an S3 storage configuration pointing internally to `http://minio:9000`, path-style access enabled, and warehouse location `s3://warehouse/`. The Trino catalog file expects a Polaris catalog named `sncf` and a client credential `root:secret`; update it to match the principal/client created in Polaris. If your Polaris build does not support the bootstrap variable shown in `.env.example`, use the official Apache Polaris quickstart bootstrap procedure for that image.

For durable catalog metadata, configure Polaris with a PostgreSQL/JDBC metastore. The included Postgres service is currently used by Airflow and is **not automatically configured as Polaris's metastore**.

The Trino connector configuration may need adjustment to match the exact Trino/Polaris versions and the chosen Polaris credential-vending setup. Keep the Polaris and Trino configuration aligned. Do not use the default passwords outside a local sandbox.

## Medallion layout

Suggested object-store paths:
- `s3://bronze/sncf/frequentation/ingestion_date=YYYY-MM-DD/` — original source files
- `s3://warehouse/silver/` — normalized Iceberg tables
- `s3://warehouse/gold/` — business marts for dashboards

The `minio-init` service creates buckets `warehouse`, `bronze`, `silver`, and `gold`.
The Iceberg catalog's `warehouse` location is configured in Polaris; it is not the same thing as the MinIO bucket itself.

## Add Python and dbt

- Put ingestion scripts in `python/`.
- Put the dbt project in `dbt/`.
- Replace the example Airflow DAG's echo commands with your real commands.
- For dbt with Trino, add `dbt-trino` to an image/environment that has dbt installed. The base Airflow image does not include dbt by default.

## Troubleshooting

- `docker compose config` validates Compose syntax.
- `docker compose logs -f polaris trino` shows catalog startup and connectivity problems.
- `docker compose logs -f airflow airflow-init` shows Airflow initialization errors.
- Check that Trino can resolve Docker service names (`polaris`, `minio`) from inside the Compose network.
