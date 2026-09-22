from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, lower


# =========================================================
# PRODUCTOS - CAPA SILVER
# =========================================================

@dp.materialized_view(
    name="producto_silver",
    comment="Capa Silver: productos limpios y estandarizados."
)
def producto_silver():
    return (
        spark.read.table("producto_bronze")
        .filter(col("producto_id").isNotNull())
        .filter(col("sku").isNotNull())
        .filter(col("precio_actual").isNotNull())
        .filter(col("precio_actual") > 0)
        .withColumn("sku", trim(col("sku")))
        .withColumn("nombre", trim(col("nombre")))
        .withColumn("estado", lower(trim(col("estado"))))
    )


# =========================================================
# INVENTARIO - CAPA SILVER
# =========================================================

@dp.materialized_view(
    name="inventario_silver",
    comment="Capa Silver: inventario limpio y validado por producto y bodega."
)
def inventario_silver():
    return (
        spark.read.table("inventario_bronze")
        .filter(col("producto_id").isNotNull())
        .filter(col("bodega_id").isNotNull())
        .filter(col("cantidad_disponible").isNotNull())
        .filter(col("cantidad_disponible") >= 0)
    )


# =========================================================
# VENTAS - CAPA SILVER
# =========================================================

@dp.materialized_view(
    name="detalle_pedido_silver",
    comment="Capa Silver: detalle de pedidos limpio y validado."
)
def detalle_pedido_silver():
    return (
        spark.read.table("detalle_pedido_bronze")
        .filter(col("detalle_pedido_id").isNotNull())
        .filter(col("pedido_id").isNotNull())
        .filter(col("producto_id").isNotNull())
        .filter(col("bodega_id").isNotNull())
        .filter(col("cantidad").isNotNull())
        .filter(col("precio_unitario").isNotNull())
        .filter(col("cantidad") > 0)
        .filter(col("precio_unitario") > 0)
    )


# =========================================================
# GENERACIÓN SOLAR - CAPA SILVER
# =========================================================

@dp.materialized_view(
    name="generacion_solar_silver",
    comment="Capa Silver: datos limpios de generación solar anual en El Salvador."
)
def generacion_solar_silver():

    datos = spark.read.table("generacion_solar_bronze")

    return (
        datos
        .filter(col("Anio").isNotNull())
        .filter(col("generacion_solar_twh").isNotNull())
        .filter(col("generacion_solar_twh") >= 0)
        .filter(col("iso3") == "SLV")
        .select(
            col("Anio").alias("anio"),
            col("generacion_solar_twh"),
            col("territorio"),
            col("iso3")
        )
        .orderBy("anio")
    )


# =========================================================
# METEOROLOGÍA - CAPA SILVER
# =========================================================

@dp.materialized_view(
    name="meteorologia_silver",
    comment="Capa Silver: datos meteorológicos limpios y tipados."
)
def meteorologia_silver():

    return (
        spark.read.table("meteorologia_bronze")
        .select(
            col("fecha_hora").cast("timestamp").alias("fecha_hora"),
            col("temperatura").cast("double").alias("temperatura"),
            col("nubosidad").cast("double").alias("nubosidad"),
            col("radiacion_solar").cast("double").alias("radiacion_solar")
        )
        .filter(col("fecha_hora").isNotNull())
    )