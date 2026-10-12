SELECT 
    CORR(radiacion_kwh_m2_dia, ingresos_totales) AS corr_radiacion_ingresos,
    CORR(radiacion_kwh_m2_dia, total_pedidos)    AS corr_radiacion_pedidos,
    CORR(radiacion_kwh_m2_dia, unidades_vendidas)  AS corr_radiacion_unidades,
    COVAR_SAMP(ingresos_totales, radiacion_kwh_m2_dia) / VAR_SAMP(radiacion_kwh_m2_dia) AS dolares_extra_por_kwh
FROM default.ventas_radiacion_diaria_gold;
