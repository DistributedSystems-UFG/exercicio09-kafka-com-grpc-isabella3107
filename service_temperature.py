from concurrent import futures
from kafka import KafkaConsumer
import grpc
import threading
import json

import TemperatureService_pb2
import TemperatureService_pb2_grpc

database = []

consumer = KafkaConsumer(
    'temperature_averages',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

def consume_kafka():

    for message in consumer:

        database.append(message.value)

        print("Stored:", message.value)

class TemperatureServer(
    TemperatureService_pb2_grpc.TemperatureServiceServicer
):

    def GetLatestTemperature(self, request, context):

        last = database[-1]

        return TemperatureService_pb2.TemperatureData(
            sensor_id=last['sensor_id'],
            average_temperature=last['average_temperature'],
            timestamp=last['timestamp']
        )

    def ListTemperatures(self, request, context):

        result = TemperatureService_pb2.TemperatureList()

        for item in database:

            temp = TemperatureService_pb2.TemperatureData(
                sensor_id=item['sensor_id'],
                average_temperature=item['average_temperature'],
                timestamp=item['timestamp']
            )

            result.data.append(temp)

        return result

def serve():

    thread = threading.Thread(target=consume_kafka)
    thread.start()

    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10)
    )

    TemperatureService_pb2_grpc.add_TemperatureServiceServicer_to_server(
        TemperatureServer(),
        server
    )

    server.add_insecure_port('[::]:50051')

    server.start()

    print("gRPC Server running...")

    server.wait_for_termination()

serve()