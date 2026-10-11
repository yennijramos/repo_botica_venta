FROM apache/airflow:2.10.2

# Instalar dependencias principales del proyecto
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --use-deprecated=legacy-resolver -r /requirements.txt

# Instalar herramientas de desarrollo y pruebas
RUN pip install --no-cache-dir pytest==8.3.3 pytest-mock==3.14.0 ruff==0.7.4


