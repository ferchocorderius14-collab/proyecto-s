-- =====================================================================
-- Proyecto Integrador Fase 2 - AES El Salvador Soluciones Solares
-- Consulta OLAP 4: Rotacion y riesgo de quiebre por producto
-- Autora: sharon ariel gomez cartagena
-- Tabla Gold: workspace.default.analisis_aes_gold
-- Objetivo: calcular el indice de rotacion de cada producto y
-- clasificarlo como riesgo de quiebre, sobreinventario, equilibrado
-- o sin movimiento (continuidad con el desafio de la Fase 1).
-- =====================================================================
WITH base AS (
    SELECT
        producto_id,
        sku,
        nombre,
        categoria_id,
        stock_total,
        COALESCE(unidades_vendidas, 0)  AS unidades_vendidas,
        COALESCE(ingresos_totales, 0)   AS ingresos_totales
    FROM workspace.default.analisis_aes_gold
)
SELECT
    producto_id,
    sku,
    nombre,
    categoria_id,
    stock_total,
    unidades_vendidas,
    ROUND(ingresos_totales, 2)                               AS ingresos_totales,
    ROUND(unidades_vendidas / NULLIF(stock_total, 0), 2)     AS indice_rotacion,
    CASE
        WHEN unidades_vendidas = 0               THEN 'sin_movimiento'
        WHEN stock_total < unidades_vendidas     THEN 'riesgo_quiebre'
        WHEN stock_total > 3 * unidades_vendidas THEN 'sobreinventario'
        ELSE 'equilibrado'
    END                                                      AS clasificacion_stock,
    RANK() OVER (ORDER BY unidades_vendidas / NULLIF(stock_total, 0) DESC NULLS LAST) AS ranking_rotacion
FROM base
ORDER BY ranking_rotacion;
