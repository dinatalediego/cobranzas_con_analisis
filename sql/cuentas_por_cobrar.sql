select
  c.nombres_cliente as cliente,
  u.codigo_proforma as unidad,
  case when u.tipo = 'estacionamiento' then 'EST' else 'DEP' end as tipo_item,
  sum(p.monto_programado) as total_por_cobrar,
  max(p.fecha_vcto) as fecha_vencimiento
from grupocygnus.pagos p
join grupocygnus.unidades u on u.id = p.unidad_id
join grupocygnus.clientes c on c.id = p.cliente_id
where p.estado in ('pendiente','por_cobrar')
group by 1,2,3;
