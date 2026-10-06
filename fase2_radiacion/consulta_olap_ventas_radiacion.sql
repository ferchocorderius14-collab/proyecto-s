SELECT
    COALESCE(nivel_radiacion, 'TOTAL') AS nivel_radiacion,
    COUNT(*) AS dias,
    SUM(total_pedidos) AS pedidos,
    SUM(unidades_vendidas) AS unidades,
    ROUND(SUM(ingresos_totales), 2) AS ingresos,
    ROUND(AVG(ingresos_totales), 2) AS ingreso_promedio_por_dia,
    ROUND(AVG(radiacion_kwh_m2_dia), 2) AS radiacion_promedio
FROM workspace.default.ventas_radiacion_diaria_gold
GROUP BY ROLLUP(nivel_radiacion)
ORDER BY nivel_radiacion