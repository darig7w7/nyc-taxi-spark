from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum, count, round, hour,
    lit, when, dayofweek, unix_timestamp, concat
)

spark = SparkSession.builder \
    .appName("NYC_Taxi_Analytics") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "50") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

print("=" * 60)
print("  NYC YELLOW TAXI — ANALISIS 2019-2026")
print("=" * 60)

# Zonas oficiales
print("\n[0] Cargando zonas...")
zones = spark.read.csv("data/taxi_zone_lookup.csv", header=True, inferSchema=True)

# Leer un año a la vez para ahorrar memoria
anios = [2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026]
resultados = []

for anio in anios:
    print(f"\nProcesando {anio}...")
    path = f"data/yellow_tripdata_{anio}-04.parquet"
    df = spark.read.parquet(path) \
        .withColumn("anio", lit(anio))

    COLS = ["anio","VendorID","tpep_pickup_datetime",
            "tpep_dropoff_datetime","passenger_count",
            "trip_distance","PULocationID","DOLocationID",
            "payment_type","fare_amount","tip_amount","total_amount"]

    df = df.select(COLS) \
        .filter(col("trip_distance") > 0) \
        .filter(col("fare_amount") > 0) \
        .filter(col("passenger_count") > 0)

    # JOIN con zonas
    df = df.join(
        zones.select(
            col("LocationID").alias("PULocationID"),
            col("Borough").alias("pickup_borough"),
            col("Zone").alias("pickup_zone")
        ), on="PULocationID", how="left"
    )

    # Métricas
    df = df.withColumn("duration_min",
        round((unix_timestamp(col("tpep_dropoff_datetime")) -
               unix_timestamp(col("tpep_pickup_datetime"))) / 60, 2)) \
        .withColumn("hora", hour(col("tpep_pickup_datetime"))) \
        .withColumn("tipo_pago",
            when(col("payment_type") == 1, "Tarjeta")
            .when(col("payment_type") == 2, "Efectivo")
            .otherwise("Otro"))

    res = df.groupBy("anio") \
        .agg(
            count("*").alias("total_viajes"),
            round(avg("fare_amount"), 2).alias("tarifa_promedio"),
            round(avg("tip_amount"), 2).alias("propina_promedio"),
            round(avg("total_amount"), 2).alias("total_promedio"),
            round(avg("trip_distance"), 2).alias("distancia_promedio"),
            round(avg("duration_min"), 2).alias("duracion_promedio")
        )

    resultados.append(res)
    print(f"   {anio} procesado")

# Combinar resultados
from functools import reduce
df_final = reduce(lambda a, b: a.union(b), resultados).orderBy("anio")

print("\n" + "=" * 60)
print("  RESULTADO FINAL")
print("=" * 60)
df_final.show()

df_final.write.mode("overwrite") \
    .partitionBy("anio") \
    .parquet("output/comparativa_anual")

