import pickle
import time
from concurrent import futures

import grpc

# import the generated classes
import MasterImpl_pb2
import MasterImpl_pb2_grpc
import Worker_pb2
import Worker_pb2_grpc


def general_worker(port):
    # Create a class to define the server functions
    class WorkerServicer(Worker_pb2_grpc.WorkerAPIServicer):

        def read_csv(self, request, context):
            worker.read_csv(request.url)
            return Worker_pb2.EmptyWorker()

        def apply(self, request, context):
            response = Worker_pb2.Dataframe()
            response.dataframe = worker.apply(request.func)
            return response

        def columns(self, request, context):
            response = Worker_pb2.Index()
            response.index = worker.columns()
            return response

        def groupby(self, request, context):
            response = Worker_pb2.Dataframe()
            response.dataframe = worker.groupby(request.by)
            return response

        def head(self, request, context):
            response = Worker_pb2.Dataframe()
            response.dataframe = worker.head(request.n)
            return response

        def isin(self, request, context):
            response = Worker_pb2.Dataframe()
            response.dataframe = worker.isin(pickle.loads(request.values))
            return response

        def items(self, request, context):
            response = Worker_pb2.Items()
            response.items = worker.items()
            return response

        def max(self, request, context):
            response = Worker_pb2.Series()
            response.series = worker.max(request.axis)
            return response

        def min(self, request, context):
            response = Worker_pb2.Series()
            response.series = worker.min(request.axis)
            return response

    # Create a gRPC-distributed-dataframe server
    worker = grpc.server(futures.ThreadPoolExecutor(max_workers=10))

    # Add the defined class to the created server
    Worker_pb2_grpc.add_WorkerAPIServicer_to_server(WorkerServicer(), worker)

    # Set up client to master
    master = MasterImpl_pb2_grpc.MasterAPIStub(grpc.insecure_channel('localhost:50050'))

    print('Starting server. Listening on port ' + str(port))
    worker.add_insecure_port('[::]:' + str(port))

    master.add_node(MasterImpl_pb2.Worker(worker='localhost:' + str(port)))
    worker.start()

    # Given server.start() will not block the execution,
    # a sleeping loop is added to keep the instance running.
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        master.remove_node(MasterImpl_pb2.Worker(worker='localhost:' + str(port)))
        worker.stop(0)
