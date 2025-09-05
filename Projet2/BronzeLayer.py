from pyspark.sql.functions import *

# ------------------- Azure Event Hub Configuration -------------------
event_hub_namespace = "Hub_namespace"
event_hub_name = "Hub_name"
event_hub_conn_str = dbutils.secrets.get(scope="vengovaultscope", key="eventhub-conn")

kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

# ------------------- Read from Event Hub -------------------
raw_df = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# Cast value as string JSON
json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

# ------------------- ADLS Configuration -------------------
storage_account_key = dbutils.secrets.get(scope="vengovaultscope", key="storage-key")
spark.conf.set(
    "fs.azure.account.key.vengoelectricalstorage.dfs.core.windows.net",
    storage_account_key
)

bronze_path = "abfss://bronze@vengoelectricalstorage.dfs.core.windows.net/bronze/ev_data"

# ------------------- Write Stream to Bronze -------------------
(
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/mnt/bronze/_checkpoints/ev_data")
    .start(bronze_path)
)



display(spark.read.format("delta").load(bronze_path))




manual_json = '{"vehicle_id": "122b690b3-7848-41a2-9db9-da9509b96f71", "vin": "VINE2128279", "make": "Voltix Test SCD2 test", "model": "V2", "year": 2022, "battery_kwh": 75, "odo_km": 142230, "driver_id": "1226fd71422-9055-4d1c-8a0f-dceedb0a7cfa", "driver_name": "Ensab Driver Test Final", "station_id": "ST034", "station_name": "Station 34", "station_power_kw": 350, "station_lat": 51.589884, "station_lon": -0.050763, "timestamp": "2025-09-01T16:30:49.731150", "speed_kmh": 62.85, "soc": 63, "power_kw": 6.68, "distance_delta_km": 0.924, "is_charging": true}'


manual_df = spark.createDataFrame([(manual_json,)],["raw_json"])

manual_df.write.format("delta").mode("append").save(bronze_path)


