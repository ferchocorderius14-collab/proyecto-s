from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(
    name="ventas_radiacion_diaria_gold",
    comment="Capa Gold: ventas diarias (sin pedidos cancelados) junto a la radiación solar del mismo día."
)
def ventas_radiacion_diaria_gold():
    base = spark.read.table("pedido_radiacion_silver").filter(F.col("estado") != "cancelado")
    return (
        base
        .groupBy("fecha", "radiacion_kwh_m2_dia", "nivel_radiacion")
        .agg(
            F.countDistinct("pedido_id").alias("total_pedidos"),
            F.sum("cantidad").alias("unidades_vendidas"),
            F.sum("importe_venta").alias("ingresos_totales"),
            F.sum("total").alias("total_registrado_pedido")
        )
        .withColumn("ticket_promedio", F.round(F.col("ingresos_totales") / F.col("total_pedidos"), 2))
        .select(
            "fecha", "total_pedidos", "unidades_vendidas", "ingresos_totales",
            "total_registrado_pedido", "ticket_promedio",
            "radiacion_kwh_m2_dia", "nivel_radiacion"
        )
        .orderBy("fecha")
    )