from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:

    event = {
        "sensor_id": 1,
        "temperature": round(random.uniform(20, 35), 2),
        "timestamp": datetime.now().isoformat()
    }

    producer.send("temperature_readings", event)

    print("Produced:", event)

    time.sleep(5)