import pytest
from dags.utils.transformaciones import normalizar_venta, calcular_margen

def venta(**overrides):
    row = {"id_venta":"BV-1", "fecha_venta":"2026-10-01", "id_producto":"MED-1",
           "producto":"Paracetamol", "cantidad":"3", "precio_unitario":"2.50", "descuento":"1.00"}
    row.update(overrides)
    return row

def test_normalizar_venta_calcula_subtotal_y_neto():
    result = normalizar_venta(venta())
    assert result["subtotal"] == 7.5
    assert result["venta_neta"] == 6.5
    assert result["cantidad"] == 3

@pytest.mark.parametrize("changes", [
    {"cantidad":"0"}, {"cantidad":"-2"}, {"precio_unitario":"-1"},
    {"descuento":"9"}, {"id_producto":""}
])
def test_normalizar_venta_rechaza_registros_invalidos(changes):
    with pytest.raises(ValueError):
        normalizar_venta(venta(**changes))

def test_calcular_margen():
    assert calcular_margen(12, 7.5) == 4.5

def test_calcular_margen_rechaza_importes_negativos():
    with pytest.raises(ValueError):
        calcular_margen(-1, 2)
