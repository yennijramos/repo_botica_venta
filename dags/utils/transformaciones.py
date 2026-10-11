from decimal import Decimal, InvalidOperation

def normalizar_venta(fila: dict) -> dict:
    """Valida y normaliza una venta de botica sin depender de Airflow."""
    required = ("id_venta", "fecha_venta", "id_producto", "producto", "cantidad", "precio_unitario", "descuento")
    missing = [k for k in required if fila.get(k) in (None, "")]
    if missing:
        raise ValueError(f"Campos obligatorios ausentes: {', '.join(missing)}")
    try:
        cantidad = int(fila["cantidad"])
        precio = Decimal(str(fila["precio_unitario"]))
        descuento = Decimal(str(fila["descuento"]))
    except (ValueError, InvalidOperation) as exc:
        raise ValueError("Cantidad, precio_unitario y descuento deben ser numéricos") from exc
    if cantidad <= 0 or precio < 0 or descuento < 0 or descuento > precio * cantidad:
        raise ValueError("Venta inválida: cantidad > 0 y descuento entre 0 y subtotal")
    return {
        **fila,
        "cantidad": cantidad,
        "precio_unitario": float(precio),
        "descuento": float(descuento),
        "subtotal": float(precio * cantidad),
        "venta_neta": float(precio * cantidad - descuento),
    }

def calcular_margen(precio_venta, costo_unitario):
    precio, costo = Decimal(str(precio_venta)), Decimal(str(costo_unitario))
    if precio < 0 or costo < 0:
        raise ValueError("Los importes no pueden ser negativos")
    return float(precio - costo)
