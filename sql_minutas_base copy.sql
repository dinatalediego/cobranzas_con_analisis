WITH 
p AS (
  -- Separación (precio depa / fecha separación)
  SELECT *
  FROM grupocygnus.procesos
  WHERE LOWER(nombre) IN ('separación','separacion')
    AND tipo_unidad_principal IN ('departamento flat','departamento duplex')
),
minuta AS (
  -- Minuta (solo ventas con fecha_fin)
  SELECT
    fecha_fin AS fecha_minuta,
    codigo_proforma,
    documento_cliente,
    codigo_unidad,
    codigo_proyecto,
    usuario_separacion,
    tipo_unidad_principal,
    estado,
    codigo_unidades_asignadas AS unidades_adicionales_raw
  FROM grupocygnus.procesos
  WHERE LOWER(nombre) = 'venta'
    AND tipo_unidad_principal IN ('departamento flat','departamento duplex')
    AND fecha_fin IS NOT NULL
),
cli AS (
  SELECT
    documento AS documento_cliente,
    nombres AS nombres_cliente,
    apellidos AS apellidos_cliente,
    canal_entrada,
    medio_captacion
  FROM grupocygnus.clientes
),
uni_depa AS (
  -- Depa principal (por codigo_unidad)
  SELECT
    codigo,
    codigo_proyecto,
    nombre_proyecto,
    nombre,
    area_techada,
    area_libre,
    tipo_unidad,
    total_habitaciones
  FROM grupocygnus.unidades
),
proy AS (
  SELECT codigo, nombre
  FROM grupocygnus.proyectos
),
minuta_x AS (
  -- Normaliza 2 posibles códigos adicionales desde unidades_adicionales_raw
  SELECT
    m.*,
    NULLIF(TRIM(split_part(m.unidades_adicionales_raw, ',', 1)), '') AS codigo_1,
    NULLIF(TRIM(split_part(m.unidades_adicionales_raw, ',', 2)), '') AS codigo_2
  FROM minuta m
),
u1 AS (
  SELECT
    codigo,
    nombre,
    tipo_unidad,
    precio_venta
  FROM grupocygnus.unidades
),
u2 AS (
  SELECT
    codigo,
    nombre,
    tipo_unidad,
    precio_venta
  FROM grupocygnus.unidades
),
final_base AS (
  SELECT
    mx.*,

    -- Trae atributos de los 2 códigos adicionales
    u1.tipo_unidad  AS tipo_1,
    u2.tipo_unidad  AS tipo_2,
    u1.nombre       AS nombre_1,
    u2.nombre       AS nombre_2,
    u1.precio_venta AS precio_1,
    u2.precio_venta AS precio_2

  FROM minuta_x mx
  LEFT JOIN u1 ON u1.codigo = mx.codigo_1
  LEFT JOIN u2 ON u2.codigo = mx.codigo_2
)

SELECT
  fb.codigo_proforma,
  fb.codigo_unidad,
  fb.documento_cliente,
  COALESCE(pr.nombre, ud.nombre_proyecto) AS proyecto,
  fb.usuario_separacion AS asesor,
  (c.nombres_cliente || ' ' || c.apellidos_cliente) AS cliente,
  c.medio_captacion AS medio_captacion,
  ud.nombre AS unidad,
  ud.total_habitaciones AS dorms,
  fb.unidades_adicionales_raw AS unidades_adicionales,
  p.precio_venta AS precio_venta_depa_soles,
  p.fecha_inicio AS fecha_separacion,
  fb.fecha_minuta,
  'Minuta' AS estado_minuta,
  /* 1) Estacionamiento */
  COALESCE(
    CASE WHEN fb.codigo_1 IS NOT NULL AND LOWER(fb.tipo_1) IN (
      'estacionamiento simple','estacionamiento doble','estacionamiento con depósito','estacionamiento con deposito'
    ) THEN fb.nombre_1 END,
    CASE WHEN fb.codigo_2 IS NOT NULL AND LOWER(fb.tipo_2) IN (
      'estacionamiento simple','estacionamiento doble','estacionamiento con depósito','estacionamiento con deposito'
    ) THEN fb.nombre_2 END
  ) AS codigo_estacionamiento_proforma,
  COALESCE(
    CASE WHEN fb.codigo_1 IS NOT NULL AND LOWER(fb.tipo_1) IN (
      'estacionamiento simple','estacionamiento doble','estacionamiento con depósito','estacionamiento con deposito'
    ) THEN fb.precio_1 END,
    CASE WHEN fb.codigo_2 IS NOT NULL AND LOWER(fb.tipo_2) IN (
      'estacionamiento simple','estacionamiento doble','estacionamiento con depósito','estacionamiento con deposito'
    ) THEN fb.precio_2 END
  ) AS precio_estacionamiento_proforma,
  /* 2) Depósito */
  COALESCE(
    CASE WHEN fb.codigo_1 IS NOT NULL AND LOWER(fb.tipo_1) IN ('depósito','deposito') THEN fb.nombre_1 END,
    CASE WHEN fb.codigo_2 IS NOT NULL AND LOWER(fb.tipo_2) IN ('depósito','deposito') THEN fb.nombre_2 END
  ) AS codigo_deposito_proforma,
  COALESCE(
    CASE WHEN fb.codigo_1 IS NOT NULL AND LOWER(fb.tipo_1) IN ('depósito','deposito') THEN fb.precio_1 END,
    CASE WHEN fb.codigo_2 IS NOT NULL AND LOWER(fb.tipo_2) IN ('depósito','deposito') THEN fb.precio_2 END
  ) AS precio_deposito_proforma,
  /* 3) Tipo compra */
  CASE
    WHEN
      COALESCE(
        CASE WHEN fb.codigo_1 IS NOT NULL AND LOWER(fb.tipo_1) IN (
          'estacionamiento simple','estacionamiento doble','estacionamiento con depósito','estacionamiento con deposito'
        ) THEN 1 END,
        CASE WHEN fb.codigo_2 IS NOT NULL AND LOWER(fb.tipo_2) IN (
          'estacionamiento simple','estacionamiento doble','estacionamiento con depósito','estacionamiento con deposito'
        ) THEN 1 END
      ) = 1
      AND
      COALESCE(
        CASE WHEN fb.codigo_1 IS NOT NULL AND LOWER(fb.tipo_1) IN ('depósito','deposito') THEN 1 END,
        CASE WHEN fb.codigo_2 IS NOT NULL AND LOWER(fb.tipo_2) IN ('depósito','deposito') THEN 1 END
      ) = 1
    THEN 'depa + estacionamiento + deposito'
    WHEN
      COALESCE(
        CASE WHEN fb.codigo_1 IS NOT NULL AND LOWER(fb.tipo_1) IN (
          'estacionamiento simple','estacionamiento doble','estacionamiento con depósito','estacionamiento con deposito'
        ) THEN 1 END,
        CASE WHEN fb.codigo_2 IS NOT NULL AND LOWER(fb.tipo_2) IN (
          'estacionamiento simple','estacionamiento doble','estacionamiento con depósito','estacionamiento con deposito'
        ) THEN 1 END
      ) = 1
    THEN 'depa + estacionamiento'
    WHEN
      COALESCE(
        CASE WHEN fb.codigo_1 IS NOT NULL AND LOWER(fb.tipo_1) IN ('depósito','deposito') THEN 1 END,
        CASE WHEN fb.codigo_2 IS NOT NULL AND LOWER(fb.tipo_2) IN ('depósito','deposito') THEN 1 END
      ) = 1
    THEN 'depa + deposito'
    ELSE 'departamento solo'
  END AS tipo_compra,
  /* 4) Precio total */
  COALESCE(p.precio_venta, 0)
  + COALESCE(
      COALESCE(
        CASE WHEN fb.codigo_1 IS NOT NULL AND LOWER(fb.tipo_1) IN (
          'estacionamiento simple','estacionamiento doble','estacionamiento con depósito','estacionamiento con deposito'
        ) THEN fb.precio_1 END,
        CASE WHEN fb.codigo_2 IS NOT NULL AND LOWER(fb.tipo_2) IN (
          'estacionamiento simple','estacionamiento doble','estacionamiento con depósito','estacionamiento con deposito'
        ) THEN fb.precio_2 END
      ),
    0)
  + COALESCE(
      COALESCE(
        CASE WHEN fb.codigo_1 IS NOT NULL AND LOWER(fb.tipo_1) IN ('depósito','deposito') THEN fb.precio_1 END,
        CASE WHEN fb.codigo_2 IS NOT NULL AND LOWER(fb.tipo_2) IN ('depósito','deposito') THEN fb.precio_2 END
      ),
    0)
  AS precio_total_venta
FROM final_base fb
LEFT JOIN cli c
  ON c.documento_cliente = fb.documento_cliente
LEFT JOIN uni_depa ud
  ON ud.codigo = fb.codigo_unidad
LEFT JOIN proy pr
  ON pr.codigo = fb.codigo_proyecto
LEFT JOIN p
  ON p.codigo_proforma = fb.codigo_proforma
WHERE fb.fecha_minuta >= DATE '2025-12-01'
  AND fb.estado = 'Activo'
ORDER BY fb.fecha_minuta ASC;
