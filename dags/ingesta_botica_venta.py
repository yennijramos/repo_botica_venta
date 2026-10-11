"""Pipeline de ingesta para BoticaVenta:
CSV de ventas + API pública OpenFDA -> MinIO -> Snowflake.
"""

import csv
import json
import urllib.request
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sensors.base import PokeReturnValue
from botocore.exceptions import ClientError
from utils.transformaciones import normalizar_venta

DATA_FILE = "/opt/airflow/data/ventas_botica.csv"
BUCKET = "botica-raw"


@dag(
    dag_id="ingesta_botica_venta",
    description=(
        "Ingiere ventas desde CSV y catálogo farmacéutico "
        "desde OpenFDA; conserva originales en MinIO y "
        "carga datos crudos a Snowflake."
    ),
    doc_md=__doc__,
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Lima"),
    schedule="0 7 * * *",
    catchup=False,
    tags=["botica-venta", "ingesta", "minio", "snowflake"],
    default_args={
        "owner": "equipo-datos",
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
    },
)
def ingesta_botica_venta():

    @task.sensor(
        poke_interval=30,
        timeout=60 * 30,
        mode="reschedule",
    )
    def esperar_archivo_ventas():
        existe = Path(DATA_FILE).exists()

        return PokeReturnValue(
            is_done=existe,
            xcom_value=DATA_FILE if existe else None,
        )

    @task
    def extraer_ventas() -> list:
        with open(
            DATA_FILE,
            newline="",
            encoding="utf-8-sig",
        ) as archivo:
            filas = [
                normalizar_venta(fila)
                for fila in csv.DictReader(archivo)
            ]

        if not filas:
            raise ValueError("El CSV de ventas está vacío")

        return filas

    @task
    def extraer_catalogo_api() -> dict:
        url = (
            "https://api.fda.gov/drug/label.json"
            "?search=openfda.product_type.exact:"
            "%22HUMAN%20PRESCRIPTION%20DRUG%22"
            "&limit=20"
        )

        solicitud = urllib.request.Request(
            url,
            headers={"User-Agent": "repo-botica-venta/1.0"},
        )

        with urllib.request.urlopen(
            solicitud,
            timeout=25,
        ) as respuesta:
            payload = json.loads(
                respuesta.read().decode("utf-8")
            )

        return {
            "source_url": url,
            "results": payload.get("results", []),
        }

    @task
    def guardar_en_minio(
        ventas: list,
        catalogo: dict,
    ) -> dict:
        from datetime import datetime

        hook = S3Hook(aws_conn_id="minio_conn")
        s3 = hook.get_conn()

        # Crear el bucket solamente si no existe.
                # Crear el bucket solamente si no existe.
        try:
            s3.head_bucket(Bucket=BUCKET)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in (
                "404",
                "NoSuchBucket",
            ):
                s3.create_bucket(Bucket=BUCKET)
            else:
                raise
        stamp = datetime.now(pendulum.timezone("UTC")).strftime(
            "%Y%m%dT%H%M%SZ"
        )

        s3.put_object(
            Bucket=BUCKET,
            Key=f"ventas/ventas_{stamp}.json",
            Body=json.dumps(
                ventas,
                ensure_ascii=False,
            ).encode("utf-8"),
        )

        s3.put_object(
            Bucket=BUCKET,
            Key=f"catalogo_openfda/catalogo_{stamp}.json",
            Body=json.dumps(
                catalogo,
                ensure_ascii=False,
            ).encode("utf-8"),
        )

        return {
            "ventas": ventas,
            "catalogo": catalogo,
            "batch_id": stamp,
        }

    @task
    def cargar_raw_snowflake(lote: dict) -> str:
        """Carga ventas evitando repetir líneas ya existentes."""

        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_default"
        )
        conn = hook.get_conn()
        cur = conn.cursor()

        ventas_insertadas = 0
        catalogo_insertado = False

        try:
            cur.execute(
                "CREATE SCHEMA IF NOT EXISTS BOTICAVENTA_DB.RAW"
            )

            cur.execute("""
                CREATE TABLE IF NOT EXISTS
                BOTICAVENTA_DB.RAW.VENTAS (
                    ID_VENTA VARCHAR,
                    FECHA_VENTA DATE,
                    ID_PRODUCTO VARCHAR,
                    PRODUCTO VARCHAR,
                    CANTIDAD NUMBER,
                    PRECIO_UNITARIO FLOAT,
                    DESCUENTO FLOAT,
                    SUBTOTAL FLOAT,
                    VENTA_NETA FLOAT,
                    BATCH_ID VARCHAR
                )
            """)

            cur.execute("""
                CREATE TABLE IF NOT EXISTS
                BOTICAVENTA_DB.RAW.CATALOGO_MEDICAMENTOS (
                    BATCH_ID VARCHAR,
                    SOURCE_URL VARCHAR,
                    PAYLOAD VARIANT
                )
            """)

            for venta in lote["ventas"]:
                valores = (
                    venta["id_venta"],
                    venta["fecha_venta"],
                    venta["id_producto"],
                    venta["producto"],
                    int(venta["cantidad"]),
                    float(venta["precio_unitario"]),
                    float(venta["descuento"]),
                    float(venta["subtotal"]),
                    float(venta["venta_neta"]),
                )

                # Comprobar si esta línea ya existe.
                # BATCH_ID se excluye porque cambia en cada ejecución.
                cur.execute("""
                    SELECT COUNT(*)
                    FROM BOTICAVENTA_DB.RAW.VENTAS
                    WHERE ID_VENTA = %s
                      AND FECHA_VENTA = %s
                      AND ID_PRODUCTO = %s
                      AND PRODUCTO = %s
                      AND CANTIDAD = %s
                      AND PRECIO_UNITARIO = %s
                      AND DESCUENTO = %s
                      AND SUBTOTAL = %s
                      AND VENTA_NETA = %s
                """, valores)

                existe = cur.fetchone()[0] > 0

                if not existe:
                    cur.execute("""
                        INSERT INTO BOTICAVENTA_DB.RAW.VENTAS (
                            ID_VENTA,
                            FECHA_VENTA,
                            ID_PRODUCTO,
                            PRODUCTO,
                            CANTIDAD,
                            PRECIO_UNITARIO,
                            DESCUENTO,
                            SUBTOTAL,
                            VENTA_NETA,
                            BATCH_ID
                        )
                        SELECT
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s
                    """, valores + (lote["batch_id"],))

                    ventas_insertadas += 1

            # Evitar insertar nuevamente el mismo lote del catálogo.
            cur.execute("""
                SELECT COUNT(*)
                FROM BOTICAVENTA_DB.RAW.CATALOGO_MEDICAMENTOS
                WHERE BATCH_ID = %s
            """, (lote["batch_id"],))

            if cur.fetchone()[0] == 0:
                cur.execute("""
                    INSERT INTO
                    BOTICAVENTA_DB.RAW.CATALOGO_MEDICAMENTOS (
                        BATCH_ID,
                        SOURCE_URL,
                        PAYLOAD
                    )
                    SELECT %s, %s, PARSE_JSON(%s)
                """, (
                    lote["batch_id"],
                    lote["catalogo"]["source_url"],
                    json.dumps(
                        lote["catalogo"]["results"]
                    ),
                ))

                catalogo_insertado = True

            conn.commit()

            return (
                f"Batch {lote['batch_id']}: "
                f"{ventas_insertadas} ventas nuevas insertadas; "
                f"catálogo "
                f"{'insertado' if catalogo_insertado else 'ya existente'}."
            )

        except Exception:
            conn.rollback()
            raise

        finally:
            cur.close()
            conn.close()

    archivo = esperar_archivo_ventas()
    ventas = extraer_ventas()
    catalogo = extraer_catalogo_api()
    lote = guardar_en_minio(ventas, catalogo)

    archivo >> ventas
    [ventas, catalogo] >> lote

    carga = cargar_raw_snowflake(lote)

    from airflow.providers.standard.operators.trigger_dagrun import (
        TriggerDagRunOperator,
    )

    disparar_dbt = TriggerDagRunOperator(
        task_id="disparar_transformacion_dbt",
        trigger_dag_id="transformacion_botica_venta",
        wait_for_completion=False,
    )

    carga >> disparar_dbt


ingesta_botica_venta()
