select
    id_venta,
    fecha_venta::date as fecha_venta,
    upper(trim(id_producto)) as id_producto,
    trim(producto) as producto,
    cantidad::integer as cantidad,
    precio_unitario::float as precio_unitario,
    coalesce(descuento, 0)::float as descuento,
    coalesce(subtotal, cantidad * precio_unitario)::float as subtotal,
    coalesce(venta_neta, cantidad * precio_unitario - coalesce(descuento, 0))::float as venta_neta,
    batch_id
from {{ source('raw', 'ventas') }}
where id_venta is not null
  and id_producto is not null
  and cantidad > 0
