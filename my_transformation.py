from pyspark import pipelines as dp
from pyspark.sql.functions import col, trim, lower


# =========================================================
# PRODUCTOS - CAPA BRONZE
# =========================================================

@dp.table(
    name="producto_bronze",
    comment="Capa Bronze: datos originales de productos extraídos desde Supabase."
)
def producto_bronze():
    return spark.read.table(
        "supabase_proyecto_catalog.catalogos.producto"
    )


# =========================================================
# PRODUCTOS - CAPA SILVER
# =========================================================

@dp.table(
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
# PRODUCTOS - CAPA GOLD
# =========================================================

@dp.table(
    name="producto_gold",
    comment="Capa Gold: resumen de productos y precios por categoría."
)
def producto_gold():
    return (
        spark.read.table("producto_silver")
        .groupBy("categoria_id")
        .agg(
            {"producto_id": "count", "precio_actual": "avg"}
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
# INVENTARIO - CAPA BRONZE
# =========================================================

@dp.table(
    name="inventario_bronze",
    comment="Capa Bronze: inventario disponible por producto y bodega extraído desde Supabase."
)
def inventario_bronze():
    return spark.read.table(
        "supabase_proyecto_catalog.inventario.inventario_bodega"
    )
    # =========================================================
# INVENTARIO - CAPA SILVER
# =========================================================

@dp.table(
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
# INVENTARIO + PRODUCTOS - CAPA GOLD
# =========================================================

@dp.table(
    name="inventario_producto_gold",
    comment="Capa Gold: análisis de inventario disponible por producto."
)
def inventario_producto_gold():

    productos = spark.read.table("producto_silver")
    inventario = spark.read.table("inventario_silver")

    return (
        inventario
        .join(productos, on="producto_id", how="inner")
        .groupBy(
            "producto_id",
            "sku",
            "nombre",
            "categoria_id",
            "precio_actual"
        )
        .agg(
            {"cantidad_disponible": "sum"}
        )
        .withColumnRenamed(
            "sum(cantidad_disponible)",
            "stock_total"
        )
        .orderBy("producto_id")
    )
    # =========================================================
# VENTAS - CAPA BRONZE
# =========================================================

@dp.table(
    name="detalle_pedido_bronze",
    comment="Capa Bronze: detalle de pedidos extraído desde Supabase."
)
def detalle_pedido_bronze():
    return spark.read.table(
        "supabase_proyecto_catalog.ventas.detalle_pedido"
    )


# =========================================================
# VENTAS - CAPA SILVER
# =========================================================

@dp.table(
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
# VENTAS + PRODUCTOS - CAPA GOLD
# =========================================================

@dp.table(
    name="ventas_producto_gold",
    comment="Capa Gold: unidades vendidas e ingresos acumulados por producto."
)
def ventas_producto_gold():

    ventas = spark.read.table("detalle_pedido_silver")
    productos = spark.read.table("producto_silver")

    return (
        ventas
        .join(productos, on="producto_id", how="inner")
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
# ANÁLISIS FINAL AES - CAPA GOLD
# =========================================================

@dp.table(
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
# GENERACIÓN SOLAR EXTERNA - CAPA BRONZE
# =========================================================

@dp.table(
    name="generacion_solar_bronze",
    comment="Capa Bronze: datos externos de generación de electricidad solar en El Salvador."
)
def generacion_solar_bronze():
    return spark.read.table(
        "dbacademy.default.generacion_solar_el_salvador_corregido"
    )
    # =========================================================
# GENERACIÓN SOLAR EXTERNA - CAPA SILVER
# =========================================================

@dp.table(
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
# GENERACIÓN SOLAR EXTERNA - CAPA GOLD
# =========================================================

@dp.table(
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