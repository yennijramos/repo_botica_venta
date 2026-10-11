select
    fecha_venta,
    id_producto,
    producto,
    segmento_venta,
    count(distinct id_venta) as numero_ventas,
    sum(cantidad) as unidades_vendidas,
    round(sum(subtotal), 2) as ventas_brutas,
    round(sum(descuento), 2) as descuentos_totales,
    round(sum(venta_neta), 2) as ventas_netas,
    round(avg(precio_neto_unitario), 2) as precio_neto_promedio
from {{ ref('int_ventas_enriquecidas') }}
group by 1, 2, 3, 4
