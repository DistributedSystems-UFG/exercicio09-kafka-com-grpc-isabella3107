import grpc

import TemperatureService_pb2
import TemperatureService_pb2_grpc

channel = grpc.insecure_channel('localhost:50051')

stub = TemperatureService_pb2_grpc.TemperatureServiceStub(channel)

response = stub.GetLatestTemperature(
    TemperatureService_pb2.Empty()
)

print("Latest temperature:")
print(response)

response = stub.ListTemperatures(
    TemperatureService_pb2.Empty()
)

print("Historical data:")
print(response)