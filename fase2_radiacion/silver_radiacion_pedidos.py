from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window


@dp.materialized_view(
    name="pedido_silver",
    comment="Capa Silver: pedidos limpios, con estado estandarizado y campos de fecha para integración."
)
def pedido_silver():
    df = spark.read.table("pedido_bronze")
    return (
        df
        .filter(F.col("pedido_id").isNotNull())
        .filter(F.col("fecha_pedido").isNotNull())
        .filter(F.col("cantidad").isNotNull() & (F.col("cantidad") > 0))
        .filter(F.col("total").isNotNull() & (F.col("total") > 0))
        .withColumn("estado", F.lower(F.trim(F.col("estado"))))
        .withColumn("anio", F.year("fecha_pedido"))
        .withColumn("mes", F.month("fecha_pedido"))
        .withColumn("fecha", F.to_date("fecha_pedido"))
        .withColumn("fecha_mes", F.date_trunc("month", F.col("fecha_pedido")).cast("date"))
        .dropDuplicates(["pedido_id"])
    )


@dp.materialized_view(
    name="radiacion_solar_api_silver",
    comment="Capa Silver: radiación solar mensual limpia (sin promedios anuales ni datos faltantes)."
)
def radiacion_solar_api_silver():
    df = spark.read.table("radiacion_solar_api_bronze")
    return (
        df
        .filter(F.col("periodo").isNotNull())
        .withColumn("anio", F.substring("periodo", 1, 4).cast("int"))
        .withColumn("mes", F.substring("periodo", 5, 2).cast("int"))
        .filter(F.col("mes").between(1, 12))
        .filter(F.col("radiacion_kwh_m2_dia").isNotNull())
        .filter(F.col("radiacion_kwh_m2_dia") != -999)
        .filter(F.col("radiacion_kwh_m2_dia") > 0)
        .withColumn("fecha_mes", F.make_date(F.col("anio"), F.col("mes"), F.lit(1)))
        .dropDuplicates(["anio", "mes"])
        .select("anio", "mes", "fecha_mes", "radiacion_kwh_m2_dia", "fuente", "fecha_ingesta")
    )


@dp.materialized_view(
    name="radiacion_solar_diaria_api_silver",
    comment="Capa Silver: radiación solar diaria limpia, con fecha en formato date."
)
def radiacion_solar_diaria_api_silver():
    df = spark.read.table("radiacion_solar_diaria_api_bronze")
    return (
        df
        .filter(F.col("dia").isNotNull())
        .withColumn("fecha", F.to_date(F.col("dia"), "yyyyMMdd"))
        .filter(F.col("fecha").isNotNull())
        .filter(F.col("radiacion_kwh_m2_dia").isNotNull())
        .filter(F.col("radiacion_kwh_m2_dia") != -999)
        .filter(F.col("radiacion_kwh_m2_dia") > 0)
        .dropDuplicates(["fecha"])
        .select("fecha", "radiacion_kwh_m2_dia", "fuente", "fecha_ingesta")
    )


@dp.materialized_view(
    name="pedido_radiacion_silver",
    comment="Capa Silver: integración de pedidos y detalle con la radiación solar diaria (NASA POWER) por fecha."
)
def pedido_radiacion_silver():
    pedidos = spark.read.table("pedido_silver").select("pedido_id", "fecha", "estado", "total")
    detalle = spark.read.table("detalle_pedido_silver").select("pedido_id", "producto_id", "cantidad", "precio_unitario")

    radiacion = (
        spark.read.table("radiacion_solar_diaria_api_silver")
        .withColumn("radiacion_promedio_mes", F.avg("radiacion_kwh_m2_dia").over(Window.partitionBy()))
        .withColumn(
            "nivel_radiacion",
            F.when(F.col("radiacion_kwh_m2_dia") >= F.col("radiacion_promedio_mes"), "sobre_promedio")
             .otherwise("bajo_promedio")
        )
        .select("fecha", "radiacion_kwh_m2_dia", "nivel_radiacion")
    )

    return (
        pedidos
        .join(detalle, on="pedido_id", how="inner")
        .join(radiacion, on="fecha", how="left")
        .withColumn("importe_venta", F.col("cantidad") * F.col("precio_unitario"))
    )
