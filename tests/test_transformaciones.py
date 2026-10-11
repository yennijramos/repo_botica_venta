import pytest

from dags.utils.transformaciones import calcular_margen, normalizar_venta


def venta(**overrides):
    row = {
        "id_venta": "BV-1",
        "fecha_venta": "2026-10-01",
        "id_producto": "MED-1",
        "producto": "Paracetamol",
        "cantidad": "3",
        "precio_unitario": "2.50",
        "descuento": "1.00",
    }
    row.update(overrides)
    return row


def test_normalizar_venta_calcula_subtotal_y_neto():
    resultado = normalizar_venta(venta())
    assert resultado["subtotal"] == pytest.approx(7.5)
    assert resultado["venta_neta"] == pytest.approx(6.5)
    assert resultado["cantidad"] == 3


@pytest.mark.parametrize(
    "cambios",
    [
        {"cantidad": "0"},
        {"cantidad": "-2"},
        {"precio_unitario": "-1"},
        {"descuento": "9"},
        {"id_producto": ""},
    ],
)
def test_normalizar_venta_rechaza_registros_invalidos(cambios):
    with pytest.raises(ValueError):
        normalizar_venta(venta(**cambios))


def test_calcular_margen():
    assert calcular_margen(12, 7.5) == pytest.approx(4.5)


def test_calcular_margen_rechaza_importes_negativos():
    with pytest.raises(ValueError):
        calcular_margen(-1, 2)
