from pyspark.sql import functions as F
from pyspark.sql.functions import (
    lit, col, expr, current_timestamp, to_timestamp,
    sha2, concat_ws, coalesce, monotonically_increasing_id
)
from delta.tables import DeltaTable

from pyspark.sql import Window

# ---------------- ADLS Config ----------------
storage_account_key = dbutils.secrets.get(scope="vengovaultscope", key="storage-key")
spark.conf.set(
    "Storage_account",
    storage_account_key
)

# ---------------- Paths ----------------
silver_path = "Silver_path"
gold_dim_vehicle = "dim_vehicule"
gold_dim_driver = "dim_driver"
gold_dim_station = "dim_station"
gold_dim_time = "dim_time"
gold_fact_trip = "fact_trip"

# ---------------- Read Silver ----------------
silver_df = spark.read.format("delta").load(silver_path)
silver_df = silver_df.withColumn("timestamp", to_timestamp("timestamp"))


# Window to get latest row per driver_id
w_driver = Window.partitionBy("driver_id").orderBy(F.col("effective_from").desc())



# ==========================================================
# VEHICLE DIM (simple overwrite)
# ==========================================================
incoming_vehicle = silver_df.select("vehicle_id", "vin", "make", "model", "year", "battery_kwh", "odo_km").dropDuplicates(["vehicle_id"])
incoming_vehicle.write.format("delta").mode("overwrite").save(gold_dim_vehicle)

# ==========================================================
# DRIVER DIM (SCD2 )
# ==========================================================
# Prepare incoming driver with hash
incoming_driver = (silver_df
    .select("driver_id", "driver_name")
    .withColumn("effective_from", F.current_timestamp())
    .withColumn("_hash", F.sha2(F.concat_ws("||", F.coalesce(F.col("driver_name"), F.lit("NA"))), 256))
)

# Keep only latest row per driver_id
w_driver = Window.partitionBy("driver_id").orderBy(F.col("effective_from").desc())
incoming_driver = incoming_driver.withColumn("row_num", F.row_number().over(w_driver)) \
                                 .filter(F.col("row_num") == 1) \
                                 .drop("row_num")


# init if missing
if not DeltaTable.isDeltaTable(spark, gold_dim_driver):
    (incoming_driver
        .withColumn("surrogate_key", monotonically_increasing_id())
        .withColumn("effective_to", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
        .write.format("delta").mode("overwrite").save(gold_dim_driver)
    )
else:
    # compute hash on incoming
    incoming_driver = incoming_driver.withColumn(
        "_hash",
        sha2(concat_ws("||", coalesce(col("driver_name"), lit("NA"))), 256)
    )

    target_driver_df = spark.read.format("delta").load(gold_dim_driver).withColumn(
        "_target_hash",
        sha2(concat_ws("||", coalesce(col("driver_name"), lit("NA"))), 256)
    )

    incoming_driver.createOrReplaceTempView("incoming_driver_tmp")
    target_driver_df.createOrReplaceTempView("target_driver_tmp")

    # 1) find changed
    changes_df = spark.sql("""
        SELECT t.surrogate_key, t.driver_id
        FROM target_driver_tmp t
        JOIN incoming_driver_tmp i
          ON t.driver_id = i.driver_id
        WHERE t.is_current = true AND t._target_hash <> i._hash
    """)

    changed_keys = [row["surrogate_key"] for row in changes_df.collect()]
    target_driver = DeltaTable.forPath(spark, gold_dim_driver)

    if changed_keys:
        target_driver.update(
            condition=expr("is_current = true AND surrogate_key IN ({})".format(",".join([str(k) for k in changed_keys]))),
            set={
                "is_current": expr("false"),
                "effective_to": expr("current_timestamp()")
            }
        )

    # 2) insert new rows (new + changed)
    inserts_df = spark.sql("""
        SELECT i.driver_id, i.driver_name, i.effective_from, i._hash
        FROM incoming_driver_tmp i
        LEFT JOIN target_driver_tmp t
          ON i.driver_id = t.driver_id AND t.is_current = true
        WHERE t.driver_id IS NULL OR t._target_hash <> i._hash
    """)

    inserts_df = (inserts_df
        .withColumn("surrogate_key", monotonically_increasing_id())
        .withColumn("effective_to", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
        .select("surrogate_key", "driver_id", "driver_name", "effective_from", "effective_to", "is_current")
    )

    if inserts_df.count() > 0:
        inserts_df.write.format("delta").mode("append").save(gold_dim_driver)

# ==========================================================
# STATION DIM (simple overwrite)
# ==========================================================
incoming_station = silver_df.select("station_id", "station_name", "station_power_kw", "station_lat", "station_lon").dropDuplicates(["station_id"])
incoming_station.write.format("delta").mode("overwrite").save(gold_dim_station)

# ==========================================================
# TIME DIM (simple overwrite)
# ==========================================================
incoming_time = (silver_df
    .select("timestamp")
    .dropDuplicates(["timestamp"])
    .withColumn("date", F.to_date("timestamp"))
    .withColumn("hour", F.hour("timestamp"))
    .withColumn("minute", F.minute("timestamp"))
    .withColumn("second", F.second("timestamp"))
    .withColumn("day_of_week", F.date_format("timestamp", "EEEE"))
    .withColumn("month", F.month("timestamp"))
    .withColumn("year", F.year("timestamp"))
    .withColumn("is_weekend", F.when(F.dayofweek("timestamp").isin(1, 7), lit(True)).otherwise(lit(False)))
)
incoming_time.withColumn("surrogate_key", monotonically_increasing_id()) \
             .write.format("delta").mode("overwrite").save(gold_dim_time)

# ==========================================================
# FACT TRIP
# ==========================================================
dim_driver_df = (spark.read.format("delta").load(gold_dim_driver)
                 .filter(col("is_current") == True)
                 .select(col("surrogate_key").alias("driver_sk"), "driver_id"))

dim_time_df = spark.read.format("delta").load(gold_dim_time).select(col("surrogate_key").alias("time_sk"), "timestamp")

fact_base = (silver_df
    .select("vehicle_id", "driver_id", "station_id", "timestamp",
            "speed_kmh", "soc", "power_kw", "distance_delta_km", "odo_km", "is_charging")
    .dropDuplicates(["timestamp", "station_id", "driver_id", "vehicle_id"])
)

fact_enriched = (fact_base
    .join(dim_driver_df, on="driver_id", how="left")
    .join(dim_time_df, on="timestamp", how="left")
)

fact_final = fact_enriched.withColumn("event_date", F.to_date("timestamp")) \
                          .withColumn("event_ingestion_time", current_timestamp()) \
                          .select(
                              monotonically_increasing_id().alias("fact_id"),
                              "vehicle_id", "station_id",
                              "driver_sk", "time_sk",
                              "timestamp", "event_date",
                              "speed_kmh", "soc", "power_kw", "distance_delta_km", "odo_km",
                              "is_charging", "event_ingestion_time"
                          )

fact_final.write.format("delta").mode("overwrite").save(gold_fact_trip)

# ---------------- Sanity Checks ----------------
print("Vehicle dim count:", spark.read.format("delta").load(gold_dim_vehicle).count())
print("Driver dim count:", spark.read.format("delta").load(gold_dim_driver).count())
print("Station dim count:", spark.read.format("delta").load(gold_dim_station).count())
print("Time dim count:", spark.read.format("delta").load(gold_dim_time).count())
print("Fact rows:", spark.read.format("delta").load(gold_fact_trip).count())




# display(spark.read.format("delta").load(gold_fact_trip).filter("driver_sk = '0'"))










# ---------------- ADLS Config ----------------
storage_account_key = dbutils.secrets.get(scope="vengovaultscope", key="storage-key")
spark.conf.set(
    "fs.azure.account.key.vengoelectricalstorage.dfs.core.windows.net",
    storage_account_key
)
gold_dim_driver = "abfss://gold@vengoelectricalstorage.dfs.core.windows.net/gold/dim_driver"
display(spark.read.format("delta").load(gold_dim_driver))







storage_account_key = dbutils.secrets.get(scope="vengovaultscope", key="storage-key")
spark.conf.set(
    "fs.azure.account.key.vengoelectricalstorage.dfs.core.windows.net",
    storage_account_key
)
gold_dim_time = "abfss://gold@vengoelectricalstorage.dfs.core.windows.net/gold/dim_time"
display(spark.read.format("delta").load(gold_dim_time))









from delta.tables import DeltaTable

delta_table = DeltaTable.forPath(spark, gold_dim_time)

# Write snapshot to Parquet
delta_table.toDF().write.format("parquet").mode("overwrite").save("abfss://gold@vengoelectricalstorage.dfs.core.windows.net/gold/dim_time_parquet/")







from pyspark.sql.functions import col

# Read existing Delta/Parquet
incoming_driver = spark.read.format("delta").load(gold_dim_driver)

# Cast boolean to int
incoming_driver = incoming_driver.withColumn("is_current", col("is_current").cast("int"))

# Save to new Parquet folder for Synapse
incoming_driver.write.format("parquet").mode("overwrite").save("abfss://gold@vengoelectricalstorage.dfs.core.windows.net/gold/dim_driver_parquet/")










from pyspark.sql.functions import col

# Read existing Delta/Parquet
incoming_driver = spark.read.format("delta").load(gold_dim_time)

# Cast boolean to int
incoming_driver = incoming_driver.withColumn("is_weekend", col("is_weekend").cast("int"))
# Save to new Parquet folder for Synapse
incoming_driver.write.format("parquet").mode("overwrite").save("abfss://gold@vengoelectricalstorage.dfs.core.windows.net/gold/dim_time_parquet/")

incoming_driver.printSchema()















from pyspark.sql.functions import col

# Read Delta (or Parquet snapshot)
incoming_time = spark.read.format("delta").load(gold_dim_time)

# Cast columns
incoming_time = incoming_time.withColumn("is_weekend", col("is_weekend").cast("int"))
incoming_time = incoming_time.withColumn("surrogate_key", col("surrogate_key").cast("long"))

# Write plain Parquet (overwrite) with simple options
incoming_time.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("compression", "uncompressed") \
    .save("abfss://gold@vengoelectricalstorage.dfs.core.windows.net/gold//dim_time_parquet_fixed/")













from pyspark.sql.functions import col

# Read existing Delta/Parquet
incoming_fact = spark.read.format("delta").load(gold_fact_trip)

# Cast boolean to int
incoming_fact = incoming_fact.withColumn("is_charging", col("is_charging").cast("int"))
# Save to new Parquet folder for Synapse
incoming_fact.write.format("parquet").mode("overwrite").save("abfss://gold@vengoelectricalstorage.dfs.core.windows.net/gold/fact_parquet/")

incoming_fact.printSchema()