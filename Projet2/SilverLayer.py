from pyspark.sql.types import *
from pyspark.sql.functions import *

# ------------------- ADLS Configuration -------------------
storage_account_key = dbutils.secrets.get(scope="vengovaultscope", key="storage-key")
spark.conf.set(
    "fs.azure.account.key.vengoelectricalstorage.dfs.core.windows.net",
    storage_account_key
)

bronze_path = "bronze_path"
silver_path = "silver_path"

# ------------------- Read from Bronze -------------------
bronze_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

# ------------------- Define Schema -------------------
schema = StructType([
    StructField("vehicle_id", StringType()),
    StructField("vin", StringType()),
    StructField("make", StringType()),
    StructField("model", StringType()),
    StructField("year", IntegerType()),
    StructField("battery_kwh", IntegerType()),
    StructField("odo_km", DoubleType()),
    StructField("driver_id", StringType()),
    StructField("driver_name", StringType()),
    StructField("station_id", StringType()),
    StructField("station_name", StringType()),
    StructField("station_power_kw", IntegerType()),
    StructField("station_lat", DoubleType()),
    StructField("station_lon", DoubleType()),
    StructField("timestamp", StringType()),
    StructField("speed_kmh", DoubleType()),
    StructField("soc", IntegerType()),
    StructField("power_kw", DoubleType()),
    StructField("distance_delta_km", DoubleType()),
    StructField("is_charging", BooleanType())
])

# ------------------- Parse JSON to Columns -------------------
parsed_df = bronze_df.withColumn("data", from_json(col("raw_json"), schema)).select("data.*")

# ------------------- Convert timestamp -------------------
clean_df = parsed_df.withColumn("timestamp", to_timestamp("timestamp"))

# ------------------- Handle invalid data -------------------
# SOC cannot be > 100
clean_df = clean_df.withColumn("soc",
                               when(col("soc") > 100, 100)
                               .otherwise(col("soc")))

# ODO cannot be negative
clean_df = clean_df.withColumn("odo_km",
                               when(col("odo_km") < 0, 0.0)
                               .otherwise(col("odo_km")))

# speed_kmh cannot be negative
clean_df = clean_df.withColumn("speed_kmh",
                               when(col("speed_kmh") < 0, 0.0)
                               .otherwise(col("speed_kmh")))     




 # ------------------- HANDLE NULL STATIONS -------------------
# When station_id is NULL, create consistent "Unknown Station" values
clean_df = clean_df.withColumn("station_id", 
                               when(col("station_id").isNull() | (col("station_id") == ""), 
                                    lit("UNKNOWN_STATION"))
                               .otherwise(col("station_id")))

clean_df = clean_df.withColumn("station_name",
                               when(col("station_id") == "UNKNOWN_STATION", 
                                    lit("Unknown Station"))
                               .when(col("station_name").isNull() | (col("station_name") == ""), 
                                     lit("Unknown Station"))
                               .otherwise(col("station_name")))

clean_df = clean_df.withColumn("station_power_kw",
                               when(col("station_id") == "UNKNOWN_STATION", 
                                    lit(0))
                               .when(col("station_power_kw").isNull(), 
                                     lit(0))
                               .otherwise(col("station_power_kw")))

clean_df = clean_df.withColumn("station_lat",
                               when(col("station_id") == "UNKNOWN_STATION", 
                                    lit(0.0))
                               .when(col("station_lat").isNull(), 
                                     lit(0.0))
                               .otherwise(col("station_lat")))

clean_df = clean_df.withColumn("station_lon",
                               when(col("station_id") == "UNKNOWN_STATION", 
                                    lit(0.0))
                               .when(col("station_lon").isNull(), 
                                     lit(0.0))
                               .otherwise(col("station_lon")))                              




# ------------------- Handle other potential NULL values -------------------
# Ensure vehicle data is complete
clean_df = clean_df.withColumn("vin", 
                               when(col("vin").isNull() | (col("vin") == ""), 
                                    concat(lit("VIN_UNKNOWN_"), col("vehicle_id")))
                               .otherwise(col("vin")))

clean_df = clean_df.withColumn("make",
                               when(col("make").isNull() | (col("make") == ""), 
                                    lit("Unknown Make"))
                               .otherwise(col("make")))

clean_df = clean_df.withColumn("model",
                               when(col("model").isNull() | (col("model") == ""), 
                                    lit("Unknown Model"))
                               .otherwise(col("model")))

clean_df = clean_df.withColumn("driver_name",
                               when(col("driver_name").isNull() | (col("driver_name") == ""), 
                                    concat(lit("Unknown Driver "), col("driver_id")))
                               .otherwise(col("driver_name")))

# Handle numeric NULLs with sensible defaults
clean_df = clean_df.withColumn("year", 
                               when(col("year").isNull(), lit(2020))
                               .otherwise(col("year")))

clean_df = clean_df.withColumn("battery_kwh", 
                               when(col("battery_kwh").isNull(), lit(50))
                               .otherwise(col("battery_kwh")))

clean_df = clean_df.withColumn("odo_km", 
                               when(col("odo_km").isNull(), lit(0.0))
                               .otherwise(col("odo_km")))

clean_df = clean_df.withColumn("speed_kmh", 
                               when(col("speed_kmh").isNull(), lit(0.0))
                               .otherwise(col("speed_kmh")))

clean_df = clean_df.withColumn("soc", 
                               when(col("soc").isNull(), lit(50))
                               .otherwise(col("soc")))

clean_df = clean_df.withColumn("power_kw", 
                               when(col("power_kw").isNull(), lit(0.0))
                               .otherwise(col("power_kw")))

clean_df = clean_df.withColumn("distance_delta_km", 
                               when(col("distance_delta_km").isNull(), lit(0.0))
                               .otherwise(col("distance_delta_km")))

clean_df = clean_df.withColumn("is_charging", 
                               when(col("is_charging").isNull(), lit(False))
                               .otherwise(col("is_charging")))


# Schema evolution: ensure all expected columns exist
expected_cols = [f.name for f in schema.fields]
for col_name in expected_cols:
    if col_name not in clean_df.columns:
        clean_df = clean_df.withColumn(col_name, lit(None))

# ------------------- Write Stream to Silver -------------------
(
    clean_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", silver_path + "_checkpoint")
    .start(silver_path)
)


display(spark.read.format("delta").load(silver_path))