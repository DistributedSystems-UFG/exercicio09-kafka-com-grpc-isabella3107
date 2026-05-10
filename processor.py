from kafka import KafkaConsumer, KafkaProducer
import json

consumer = KafkaConsumer(
    'temperature_readings',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

temps = []

for message in consumer:

    data = message.value

    temps.append(data['temperature'])

    avg = sum(temps) / len(temps)

    result = {
        "sensor_id": data['sensor_id'],
        "average_temperature": round(avg, 2),
        "timestamp": data['timestamp']
    }

    producer.send("temperature_averages", result)

    print("Processed:", result)