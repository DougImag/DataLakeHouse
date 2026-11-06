FROM apache/airflow:2.10.5

USER root
RUN python -m venv /opt/dlh-venv && chown -R airflow: /opt/dlh-venv \
 # Artefacts dbt (volume partagé avec OpenMetadata) : le volume Docker
 # hérite de ces droits à sa création. Groupe 0 en écriture pour le cas
 # où AIRFLOW_UID est différent de 50000 (Linux).
 && mkdir -p /opt/dlh/dbt-artifacts \
 && chown airflow:0 /opt/dlh/dbt-artifacts \
 && chmod 2775 /opt/dlh/dbt-artifacts

USER airflow
COPY extraction-data-api/requirements.txt /tmp/requirements-extraction.txt
COPY data-platform/dbt/requirements.txt   /tmp/requirements-dbt.txt
RUN /opt/dlh-venv/bin/pip install --no-cache-dir --upgrade pip \
 && /opt/dlh-venv/bin/pip install --no-cache-dir \
      -r /tmp/requirements-extraction.txt \
      -r /tmp/requirements-dbt.txt \
 && /opt/dlh-venv/bin/dbt --version
