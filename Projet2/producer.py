# ev_data_generator_stream.py
import json
import random
import uuid
import time
from datetime import datetime
from kafka import KafkaProducer

# ---------- CONFIG ----------
EVENTHUBS_NAMESPACE = "namespace"
EVENT_HUB_NAME = "hub"
CONNECTION_STRING = "connexion_string"
# ----------------------------

producer = KafkaProducer(
    bootstrap_servers=[f"{EVENTHUBS_NAMESPACE}:9093"],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Vehicle models
vehicle_models = [
    {"make": "Nexa", "model": "E1", "battery_kwh": 50},
    {"make": "Voltix", "model": "V2", "battery_kwh": 75},
    {"make": "Electra", "model": "X3", "battery_kwh": 100},
    {"make": "Tesla", "model": "Model Y", "battery_kwh": 82},
    {"make": "BYD", "model": "Han", "battery_kwh": 85},
]

# Random names
first_names = ["Alice", "Bob", "Charlie", "David", "Emma", "Fatima", "Georges", "Hana", "Ismael", "Julia"]
last_names = ["Dupont", "Martin", "Faure", "El Idrissi", "Nguyen", "Hernandez", "Khan", "Zhou", "Petrov", "Andersson"]

# Stations
stations = [
    {
        "station_id": f"ST{str(i).zfill(3)}",
        "name": f"Station {i}",
        "power_kw": random.choice([22, 50, 150, 350]),
        "latitude": round(51.50 + random.uniform(-0.1, 0.1), 6),
        "longitude": round(-0.1 + random.uniform(-0.1, 0.1), 6),
    }
    for i in range(1, 51)
]


def inject_dirty(record):
    """Introduce anomalies"""
    if random.random() < 0.03:
        record["odo_km"] = -1  # impossible odometer
    if random.random() < 0.03:
        record["soc"] = 150  # invalid SOC
    if random.random() < 0.03:
        record["station_id"] = None  # missing station
    return record


def generate_ev_event():
    vehicle_model = random.choice(vehicle_models)
    station = random.choice(stations)

    event = {
        # vehicle info
        "vehicle_id": str(uuid.uuid4()),
        "vin": f"VIN{uuid.uuid4().hex[:8].upper()}",
        "make": vehicle_model["make"],
        "model": vehicle_model["model"],
        "year": random.randint(2018, 2025),
        "battery_kwh": vehicle_model["battery_kwh"],
        "odo_km": random.randint(0, 200000),

        # driver info
        "driver_id": str(uuid.uuid4()),
        "driver_name": f"{random.choice(first_names)} {random.choice(last_names)}",

        # station info
        "station_id": station["station_id"],
        "station_name": station["name"],
        "station_power_kw": station["power_kw"],
        "station_lat": station["latitude"],
        "station_lon": station["longitude"],

        # telemetry
        "timestamp": datetime.utcnow().isoformat(),
        "speed_kmh": round(max(0, random.gauss(50, 20)), 2),
        "soc": random.randint(0, 100),
        "power_kw": round(max(0.0, random.gauss(20, 10)), 2),
        "distance_delta_km": round(random.uniform(0.0, 1.0), 3),
        "is_charging": random.choice([True, False])
    }

    return inject_dirty(event)


if __name__ == "__main__":
    try:
        while True:
            event = generate_ev_event()
            producer.send(EVENT_HUB_NAME, event)
            print(f"Sent to Event Hub: {event}")
            time.sleep(1)
    except KeyboardInterrupt:
        producer.close()
        print("Stopped EV producer.")
