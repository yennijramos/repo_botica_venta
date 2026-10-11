select
    id_venta, fecha_venta, id_producto, producto, cantidad,
    precio_unitario, descuento, subtotal, venta_neta, batch_id,
    case
      when venta_neta >= 500 then 'ALTA'
      when venta_neta >= 100 then 'MEDIA'
      else 'BAJA'
    end as segmento_venta,
    venta_neta / nullif(cantidad, 0) as precio_neto_unitario
from {{ ref('stg_ventas') }}
