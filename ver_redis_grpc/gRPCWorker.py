import pickle
from urllib import request, response
import grpc
from concurrent import futures
import time
import redis

# import the generated classes
import Worker_pb2
import Worker_pb2_grpc

# import the original Worker.py
import Worker


# create a class to define the server functions
class WorkerServicer(Worker_pb2_grpc.WorkerAPIServicer):

    def read_csv(self, request, context):
        Worker.read_csv(request.url)
        return Worker_pb2.EmptyWorker()

    def apply(self, request, context):
        response = Worker_pb2.Dataframe()
        response.dataframe = Worker.apply(request.func)
        return response

    def columns(self, request, context):
        response = Worker_pb2.Index()
        response.index = Worker.columns()
        return response

    def groupby(self, request, context):
        response = Worker_pb2.Dataframe()
        response.dataframe = Worker.groupby(request.by)
        return response

    def head(self, request, context):
        response = Worker_pb2.Dataframe()
        response.dataframe = Worker.head(request.n)
        return response

    def isin(self, request, context):
        response = Worker_pb2.Dataframe()
        response.dataframe = Worker.isin(pickle.loads(request.values))
        return response

    def items(self, request, context):
        response = Worker_pb2.Items()
        response.items = Worker.items()
        return response

    def max(self, request, context):
        response = Worker_pb2.Series()
        response.series = Worker.max(request.axis)
        return response

    def min(self, request, context):
        response = Worker_pb2.Series()
        response.series = Worker.min(request.axis)
        return response


# create a gRPC-distributed-dataframe server
worker = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

# add the defined class to the created server
Worker_pb2_grpc.add_WorkerAPIServicer_to_server(WorkerServicer(), worker)

# Set up client to master
master = redis.from_url('redis://localhost:6379', db=0)

# listen on port 50051
print('Starting server. Listening on port 50051')
worker.add_insecure_port('[::]:50051')

master.rpush("workers", 'localhost:50051')
worker.start()

# since server.start() will not block,
# a sleep-loop is added to keep alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    master.lrem("workers", 0, 'localhost:50051')
    worker.stop(0)
