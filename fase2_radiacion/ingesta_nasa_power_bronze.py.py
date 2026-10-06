import requests
from pyspark import pipelines as dp
from pyspark.sql import Row
from pyspark.sql.functions import lit, current_timestamp


@dp.materialized_view(
    name="radiacion_solar_api_bronze",
    comment="Capa Bronze: radiación solar mensual histórica obtenida desde la API NASA POWER para El Salvador."
)
def radiacion_solar_api_bronze():
    url = "https://power.larc.nasa.gov/api/temporal/monthly/point"
    params = {
        "parameters": "ALLSKY_SFC_SW_DWN",
        "community": "RE",
        "longitude": -89.19,
        "latitude": 13.69,
        "start": 2020,
        "end": 2026,
        "format": "JSON"
    }
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    serie = response.json()["properties"]["parameter"]["ALLSKY_SFC_SW_DWN"]

    rows = [Row(periodo=p, radiacion_kwh_m2_dia=float(v)) for p, v in serie.items()]
    return (
        spark.createDataFrame(rows)
        .withColumn("fuente", lit("NASA POWER API"))
        .withColumn("fecha_ingesta", current_timestamp())
    )


@dp.materialized_view(
    name="radiacion_solar_diaria_api_bronze",
    comment="Capa Bronze: radiación solar diaria obtenida desde la API NASA POWER para El Salvador."
)
def radiacion_solar_diaria_api_bronze():
    url = "https://power.larc.nasa.gov/api/temporal/daily/point"
    params = {
        "parameters": "ALLSKY_SFC_SW_DWN",
        "community": "RE",
        "longitude": -89.19,
        "latitude": 13.69,
        "start": "20260801",
        "end": "20260831",
        "format": "JSON"
    }
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()
    serie = response.json()["properties"]["parameter"]["ALLSKY_SFC_SW_DWN"]

    rows = [Row(dia=d, radiacion_kwh_m2_dia=float(v)) for d, v in serie.items()]
    return (
        spark.createDataFrame(rows)
        .withColumn("fuente", lit("NASA POWER API diaria"))
        .withColumn("fecha_ingesta", current_timestamp())
    )