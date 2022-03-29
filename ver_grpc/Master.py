from urllib import request, response
import grpc
from concurrent import futures
import time

# import the generated classes
import MasterImpl_pb2
import MasterImpl_pb2_grpc

# import the original Worker.py
import MasterImpl


# create a class to define the server functions
class MasterImplServicer(MasterImpl_pb2_grpc.MasterAPIServicer):

    def add_node(self, request, context):
        MasterImpl.add_node(request.worker)
        return MasterImpl_pb2.EmptyMaster()

    def remove_node(self, request, context):
        MasterImpl.remove_node(request.worker)
        return MasterImpl_pb2.EmptyMaster()

    def get_workers(self, request, context):
        response = MasterImpl_pb2.WorkerList()
        response.workerList[:] = MasterImpl.get_workers()
        return response


# create a gRPC-distributed-dataframe server
server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

# add the defined class to the created server
MasterImpl_pb2_grpc.add_MasterAPIServicer_to_server(
    MasterImplServicer(), server)

# listen on port 50052
print('Starting server. Listening on port 50050.')
server.add_insecure_port('[::]:50050')
server.start()

# since server.start() will not block,
# a sleep-loop is added to keep alive
try:
    while True:
        time.sleep(86400)
except KeyboardInterrupt:
    server.stop(0)
