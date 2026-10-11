FROM apache/airflow:2.10.2

# Dependencias adicionales de la Clase 7 (Dbt + adapter de Snowflake + Cosmos)
COPY requirements.txt /requirements.txt

# Forzamos la instalación lineal directa con las versiones fijas anteriores
RUN pip install --no-cache-dir --use-deprecated=legacy-resolver -r /requirements.txt


