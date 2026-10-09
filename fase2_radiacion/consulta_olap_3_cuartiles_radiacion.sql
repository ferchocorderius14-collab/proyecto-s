-- Proyecto Integrador Fase 2 - AES El Salvador Soluciones Solares
-- Consulta OLAP 3: Ventas por cuartil de radiación
-- Autor: Alex Mauricio García Maravilla
-- Tabla Gold: workspace.default.ventas_radiacion_diaria_gold
-- Objetivo: comparar ingreso diario y ticket promedio entre cuatro grupos de días según su radiación solar.

WITH dias AS (
  SELECT
    fecha,
    radiacion_kwh_m2_dia,
    ingresos_totales,
    total_pedidos,
    NTILE(4) OVER (ORDER BY radiacion_kwh_m2_dia) AS cuartil_radiacion
  FROM workspace.default.ventas_radiacion_diaria_gold
  WHERE radiacion_kwh_m2_dia IS NOT NULL
)
SELECT
  cuartil_radiacion,
  CASE cuartil_radiacion
    WHEN 1 THEN 'Q1 - menos soleados'
    WHEN 2 THEN 'Q2 - soleado medio-bajo'
    WHEN 3 THEN 'Q3 - soleado medio-alto'
    WHEN 4 THEN 'Q4 - más soleados'
  END AS grupo,
  COUNT(*) AS dias,
  ROUND(MIN(radiacion_kwh_m2_dia), 2) AS radiacion_min,
  ROUND(MAX(radiacion_kwh_m2_dia), 2) AS radiacion_max,
  ROUND(AVG(ingresos_totales), 2) AS ingreso_diario_promedio,
  ROUND(AVG(total_pedidos), 1) AS pedidos_por_dia,
  ROUND(SUM(ingresos_totales) / SUM(total_pedidos), 2) AS ticket_promedio
FROM dias
GROUP BY cuartil_radiacion
ORDER BY cuartil_radiacion;
