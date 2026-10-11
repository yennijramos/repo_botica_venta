# BoticaVenta — Pipeline integral de datos con Apache Airflow

Proyecto final adaptado al caso de negocio de una botica. El pipeline consolida ventas desde un CSV operativo y consulta un catálogo público de medicamentos mediante OpenFDA. Los archivos originales se guardan en MinIO, los datos se cargan en Snowflake y dbt construye indicadores diarios por producto.

> **Importante:** la estructura y el código inicial están preparados, pero la ejecución real contra OpenFDA, MinIO y Snowflake requiere configurar las conexiones y credenciales descritas abajo. No se incluyen credenciales reales ni se afirma que el entorno externo ya haya sido probado.

## Arquitectura

1. **Fuente 1 — CSV de ventas:** `data/ventas_botica.csv`, detectado por un sensor de Airflow.
2. **Fuente 2 — API OpenFDA:** extracción de registros de etiquetado de medicamentos; requiere conexión a Internet.
3. **Staging intermedio — MinIO:** guarda copias JSON por lote en el bucket `botica-raw`.
4. **Warehouse — Snowflake:** carga los registros de venta en `BOTICAVENTA_DB.RAW.VENTAS` y la respuesta de API en `RAW.CATALOGO_MEDICAMENTOS`.
5. **Transformación — dbt + Cosmos:** staging → intermediate → mart.
6. **Consumo:** `BOTICAVENTA_DB.MARTS.MART_VENTAS_DIARIAS`, con ventas netas, unidades y descuentos por día/producto.

```text
CSV ventas ─┐
            ├─ Airflow ──> MinIO (JSON crudo) ──> Snowflake RAW
API OpenFDA ┘                                      │
                                                  dbt + Cosmos
                                                   ↓
                                STAGING → INTERMEDIATE → MARTS
                                                   ↓
                                    Indicadores diarios por producto
```

## Estructura del repositorio

- `Dockerfile`, `docker-compose.yaml`: Airflow, Postgres, Redis y MinIO.
- `dags/ingesta_botica_venta.py`: sensor, extracción de dos fuentes, persistencia en MinIO y carga a Snowflake.
- `dags/dag_transformacion_botica.py`: orquestación dbt con Cosmos.
- `dags/dbt/botica_venta/`: proyecto dbt con tres modelos encadenados y tests.
- `dags/utils/transformaciones.py`: reglas de negocio puras, probables mediante pytest.
- `tests/`: tests unitarios y contrato del proyecto.
- `.github/workflows/`: CI en PR/push y ejemplo de CD configurable.
- `setup_snowflake.sql`: objetos iniciales del warehouse.

## Requisitos previos

Docker Desktop con 4–8 GB de memoria, Git, Python 3.11+ y una cuenta de Snowflake. La API OpenFDA es pública. MinIO se levanta localmente mediante Docker Compose.

## Puesta en marcha local

En PowerShell, desde la raíz del repositorio:

```powershell
Copy-Item .env.example .env
# Edita .env y completa SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER y SNOWFLAKE_PASSWORD
docker compose build
docker compose up airflow-init
docker compose up -d
docker compose ps
```

- Airflow: http://localhost:8080 (usuario/clave local por defecto: `airflow` / `airflow`).
- MinIO Console: http://localhost:9001 (valores locales definidos en `.env`).
- El CSV de muestra ya está en `data/ventas_botica.csv`; para simular un archivo nuevo, reemplázalo o agrega un archivo con el mismo nombre.

### Configurar Snowflake

1. En Snowsight, ejecuta `setup_snowflake.sql` con un rol autorizado.
2. Reemplaza `TU_USUARIO_SNOWFLAKE` en la última instrucción SQL.
3. En Airflow crea la Connection `snowflake_default` de tipo **Snowflake** con tu usuario, contraseña, account, warehouse `WH_BOTICAVENTA`, database `BOTICAVENTA_DB`, schema `RAW` y role `ROLE_BOTICAVENTA`.
4. Completa las tres variables `SNOWFLAKE_*` en `.env`; `profiles.yml` las lee mediante `env_var()`.

### Configurar MinIO

Compose declara la conexión Airflow `minio_conn` (`aws`) usando las credenciales locales del `.env` y el endpoint `http://minio:9000`. El DAG crea el bucket `botica-raw` si aún no existe. Cambia las credenciales por defecto si expones el servicio fuera de tu máquina.

### Ejecutar

1. En Airflow, activa `ingesta_botica_venta`. Su horario está configurado a las 07:00 hora del scheduler; `catchup=False`.
2. Cuando la ingesta haya creado las tablas RAW, configura y activa `transformacion_botica_venta` para construir los modelos dbt.
3. Consulta el resultado:

```sql
SELECT *
FROM BOTICAVENTA_DB.MARTS.MART_VENTAS_DIARIAS
ORDER BY FECHA_VENTA DESC, VENTAS_NETAS DESC;
```

> El DAG de transformación es diario e independiente; para producción conviene encadenarlo al final de la ingesta y ejecutar dbt solo después de que la carga RAW haya finalizado. La estructura actual permite ejecutar ambos DAGs por separado para facilitar la validación inicial.

## Testing

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r requirements-dev.txt
ruff check dags/ tests/
pytest tests/ -v
```

Los tests cubren reglas de validación reales (cantidades, descuentos, campos obligatorios), cálculo de negocio y estructura de los tres modelos dbt.

## CI/CD y GitHub

- `ci.yml` ejecuta lint y tests en Pull Requests hacia `main` y pushes a `main`.
- En GitHub configura Branch Protection para `main`, exige Pull Request y el check `lint-and-test`.
- Para habilitar el CD de ejemplo, configura el Environment `produccion`, las variables `PROD_HOST`, `PROD_USER` y el secret `PRODUCCION_SSH_KEY`. El servidor debe aceptar SSH y tener permisos para escribir en `~/repo_botica_venta`.
- El historial de commits debe reflejar trabajo incremental; crear el ZIP no sustituye el flujo Git real exigido por el curso.

## Seguridad y límites conocidos

- Nunca subas `.env`, claves SSH, contraseñas o tokens.
- `minioadmin` es solo una credencial local de desarrollo; no usar en producción.
- La carga a Snowflake usa la Connection `snowflake_default`; dbt usa variables de entorno, no secretos escritos en `profiles.yml`.
- La API OpenFDA puede cambiar, limitar solicitudes o no responder. La tarea falla de forma explícita y Airflow aplica reintentos.
- El CSV de muestra y la llamada a OpenFDA sirven para desarrollar el flujo; verifica el esquema y los permisos de tu cuenta Snowflake antes de la demo.

## Glosario

- **Venta neta:** subtotal de unidades por precio menos descuentos.
- **Staging:** capa inicial donde los datos se tipan y limpian.
- **Warehouse:** plataforma analítica donde se consultan datos integrados.
- **Batch:** conjunto de registros procesados en una ejecución.
- **Linaje:** relación entre fuentes, transformaciones y tablas de consumo.
