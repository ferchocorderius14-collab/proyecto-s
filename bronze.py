import requests

from pyspark import pipelines as dp
from pyspark.sql import Row


# =========================================================
# CAPA BRONZE
# Datos originales obtenidos desde fuentes externas
# =========================================================


# =========================================================
# PRODUCTOS - BRONZE
# =========================================================

@dp.materialized_view(
    name="producto_bronze",
    comment="Capa Bronze: datos originales de productos extraídos desde Supabase."
)
def producto_bronze():
    return spark.read.table(
        "supabase_proyecto_catalog.catalogos.producto"
    )


# =========================================================
# INVENTARIO - BRONZE
# =========================================================

@dp.materialized_view(
    name="inventario_bronze",
    comment="Capa Bronze: inventario original por producto y bodega extraído desde Supabase."
)
def inventario_bronze():
    return spark.read.table(
        "supabase_proyecto_catalog.inventario.inventario_bodega"
    )


# =========================================================
# VENTAS - BRONZE
# =========================================================

@dp.materialized_view(
    name="detalle_pedido_bronze",
    comment="Capa Bronze: detalle de pedidos original extraído desde Supabase."
)
def detalle_pedido_bronze():
    return spark.read.table(
        "supabase_proyecto_catalog.ventas.detalle_pedido"
    )


# =========================================================
# GENERACIÓN SOLAR EXTERNA - BRONZE
# =========================================================

@dp.materialized_view(
    name="generacion_solar_bronze",
    comment="Capa Bronze: datos externos de generación de electricidad solar en El Salvador."
)
def generacion_solar_bronze():
    return spark.read.table(
        "dbacademy.default.generacion_solar_el_salvador_corregido"
    )


# =========================================================
# METEOROLOGÍA - BRONZE
# API Open-Meteo
# =========================================================

@dp.materialized_view(
    name="meteorologia_bronze",
    comment="Capa Bronze: datos meteorológicos obtenidos desde la API Open-Meteo para El Salvador."
)
def meteorologia_bronze():

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": 13.6929,
        "longitude": -89.2182,
        "hourly": "temperature_2m,cloud_cover,shortwave_radiation",
        "timezone": "America/El_Salvador",
        "forecast_days": 7
    }

    response = requests.get(
        url,
        params=params,
        timeout=30
    )

    response.raise_for_status()

    data = response.json()
    hourly = data["hourly"]

    rows = []

    for i in range(len(hourly["time"])):
        rows.append(
            Row(
                fecha_hora=hourly["time"][i],
                temperatura=hourly["temperature_2m"][i],
                nubosidad=hourly["cloud_cover"][i],
                radiacion_solar=hourly["shortwave_radiation"][i]
            )
        )

    return (
        spark.createDataFrame(rows)
        .dropDuplicates(["fecha_hora"])
    )