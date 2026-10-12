-- =====================================================================
-- Proyecto Integrador Fase 2 - AES El Salvador Soluciones Solares
-- Consulta OLAP 5: Stock frente a ingresos por categoria
-- Autora: Pamela Alexandra Pereira Acevedo
-- Tabla Gold: workspace.default.analisis_aes_gold
-- Objetivo: identificar categorias que concentran inventario pero no
-- ingresos (capital inmovilizado en stock que no se vende).
-- =====================================================================
WITH por_categoria AS (
    SELECT
        categoria_id,
        COUNT(*)                                       AS productos,
        SUM(stock_total)                               AS stock_total,
        SUM(COALESCE(unidades_vendidas, 0))            AS unidades_vendidas,
        SUM(COALESCE(ingresos_totales, 0))             AS ingresos_totales
    FROM workspace.default.analisis_aes_gold
    GROUP BY categoria_id
),
participacion AS (
    SELECT
        *,
        ROUND(unidades_vendidas / NULLIF(stock_total, 0), 2)              AS indice_rotacion,
        ROUND(100 * stock_total / SUM(stock_total) OVER (), 2)            AS pct_del_stock,
        ROUND(100 * ingresos_totales / SUM(ingresos_totales) OVER (), 2)  AS pct_de_ingresos
    FROM por_categoria
)
SELECT
    categoria_id,
    productos,
    stock_total,
    unidades_vendidas,
    ROUND(ingresos_totales, 2)                      AS ingresos_totales,
    indice_rotacion,
    pct_del_stock,
    pct_de_ingresos,
    ROUND(pct_del_stock - pct_de_ingresos, 2)       AS brecha_stock_vs_ingresos
FROM participacion
ORDER BY brecha_stock_vs_ingresos DESC;
