from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

def test_hay_tres_modelos_dbt_encadenados():
    models = ROOT / "dags" / "dbt" / "botica_venta" / "models"
    assert (models / "staging" / "stg_ventas.sql").exists()
    assert (models / "intermediate" / "int_ventas_enriquecidas.sql").exists()
    assert (models / "marts" / "mart_ventas_diarias.sql").exists()
    assert "ref('stg_ventas')" in (models / "intermediate" / "int_ventas_enriquecidas.sql").read_text()
    assert "ref('int_ventas_enriquecidas')" in (models / "marts" / "mart_ventas_diarias.sql").read_text()

def test_no_hay_secretos_reales_en_profiles():
    profiles = (ROOT / "dags" / "dbt" / "botica_venta" / "profiles.yml").read_text()
    assert "env_var('SNOWFLAKE_PASSWORD')" in profiles
    assert "password: MiClave" not in profiles
