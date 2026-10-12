from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    avg,
    max as spark_max,
    to_date
)


# =========================================================
# CAPA GOLD
# Datos preparados para análisis
# =========================================================


# =========================================================
# PRODUCTOS - GOLD
# =========================================================

@dp.materialized_view(
    name="producto_gold",
    comment="Capa Gold: resumen de productos y precios por categoría."
)
def producto_gold():
    return (
        spark.read.table("producto_silver")
        .groupBy("categoria_id")
        .agg(
            {
                "producto_id": "count",
                "precio_actual": "avg"
            }
        )
        .withColumnRenamed(
            "count(producto_id)",
            "cantidad_productos"
        )
        .withColumnRenamed(
            "avg(precio_actual)",
            "precio_promedio"
        )
        .orderBy("categoria_id")
    )


# =========================================================
# INVENTARIO + PRODUCTOS - GOLD
# =========================================================

@dp.materialized_view(
    name="inventario_producto_gold",
    comment="Capa Gold: análisis de inventario disponible por producto."
)
def inventario_producto_gold():

    productos = spark.read.table("producto_silver")
    inventario = spark.read.table("inventario_silver")

    return (
        inventario
        .join(
            productos,
            on="producto_id",
            how="inner"
        )
        .groupBy(
            "producto_id",
            "sku",
            "nombre",
            "categoria_id",
            "precio_actual"
        )
        .agg(
            {
                "cantidad_disponible": "sum"
            }
        )
        .withColumnRenamed(
            "sum(cantidad_disponible)",
            "stock_total"
        )
        .orderBy("producto_id")
    )


# =========================================================
# VENTAS + PRODUCTOS - GOLD
# =========================================================

@dp.materialized_view(
    name="ventas_producto_gold",
    comment="Capa Gold: unidades vendidas e ingresos acumulados por producto."
)
def ventas_producto_gold():

    ventas = spark.read.table("detalle_pedido_silver")
    productos = spark.read.table("producto_silver")

    return (
        ventas
        .join(
            productos,
            on="producto_id",
            how="inner"
        )
        .withColumn(
            "importe_venta",
            col("cantidad") * col("precio_unitario")
        )
        .groupBy(
            "producto_id",
            "sku",
            "nombre",
            "categoria_id"
        )
        .agg(
            {
                "cantidad": "sum",
                "importe_venta": "sum"
            }
        )
        .withColumnRenamed(
            "sum(cantidad)",
            "unidades_vendidas"
        )
        .withColumnRenamed(
            "sum(importe_venta)",
            "ingresos_totales"
        )
        .orderBy("producto_id")
    )


# =========================================================
# ANÁLISIS FINAL AES - GOLD
# =========================================================

@dp.materialized_view(
    name="analisis_aes_gold",
    comment="Capa Gold final: análisis integrado de productos, inventario y ventas para AES."
)
def analisis_aes_gold():

    inventario = spark.read.table("inventario_producto_gold")
    ventas = spark.read.table("ventas_producto_gold")

    return (
        inventario
        .join(
            ventas.select(
                "producto_id",
                "unidades_vendidas",
                "ingresos_totales"
            ),
            on="producto_id",
            how="left"
        )
        .select(
            "producto_id",
            "sku",
            "nombre",
            "categoria_id",
            "precio_actual",
            "stock_total",
            "unidades_vendidas",
            "ingresos_totales"
        )
        .orderBy("producto_id")
    )


# =========================================================
# GENERACIÓN SOLAR EXTERNA - GOLD
# =========================================================

@dp.materialized_view(
    name="generacion_solar_gold",
    comment="Capa Gold: evolución anual de la generación de energía solar en El Salvador."
)
def generacion_solar_gold():

    datos = spark.read.table("generacion_solar_silver")

    return (
        datos
        .select(
            "anio",
            "generacion_solar_twh",
            "territorio",
            "iso3"
        )
        .orderBy("anio")
    )


# =========================================================
# METEOROLOGÍA - GOLD
# =========================================================

@dp.materialized_view(
    name="meteorologia_gold",
    comment="Capa Gold: resumen diario de temperatura, pico de calor, nubosidad y radiación solar."
)
def meteorologia_gold():

    datos = spark.read.table("meteorologia_silver")

    return (
        datos
        .withColumn(
            "fecha",
            to_date("fecha_hora")
        )
        .groupBy("fecha")
        .agg(
            avg("temperatura").alias("temperatura_promedio"),
            spark_max("temperatura").alias("pico_calor"),
            avg("nubosidad").alias("nubosidad_promedio"),
            avg("radiacion_solar").alias("radiacion_promedio")
        )
        .orderBy("fecha")
    )