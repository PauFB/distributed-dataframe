import pickle
import sys
import time
import grpc
import pandas as pd

# import the generated classes
import MasterImpl_pb2
import MasterImpl_pb2_grpc
import Worker_pb2
import Worker_pb2_grpc

start = time.time()

master = MasterImpl_pb2_grpc.MasterAPIStub(grpc.insecure_channel('localhost:50050'))

# Get workers
workers_list = list()
workers_list[:] = master.get_workers(MasterImpl_pb2.EmptyMaster()).workerList
client_worker_list = list()

for worker in workers_list:
    client_worker_list.append(Worker_pb2_grpc.WorkerAPIStub(grpc.insecure_channel(worker)))

# Read csv
csv_list = ('../df1.csv', '../df2.csv', '../df3.csv')

i = 0
for worker in client_worker_list:
    worker.read_csv(Worker_pb2.Url(url=csv_list.__getitem__(i)))
    i += 1

# Apply
print("\nTest apply(lambda x: x + 2)")
result = pd.DataFrame(columns=['x', 'y', 'z'])
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.apply(Worker_pb2.Func(func="lambda x: x + 2")).dataframe)])
print(result)

# Columns
print("\nTest columns()")
result = pd.Index([])
for worker in client_worker_list:
    result = result.union(pickle.loads(worker.columns(Worker_pb2.EmptyWorker()).index))
print(result)

# GroupBy
print("\nTest groupby(z).sum()")
result = pd.DataFrame(columns=['x', 'y'])
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.groupby(Worker_pb2.By(by='z')).dataframe).sum()])
print(result)

# Head
print("\nTest head()")
result = pd.DataFrame(columns=['x', 'y', 'z'])
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.head(Worker_pb2.N(n=4)).dataframe)])
print(result)

# Isin
print("\nTest isin([2, 4])")
result = pd.DataFrame(columns=['x', 'y', 'z'])
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.isin(Worker_pb2.Values(values=pickle.dumps([2, 4]))).dataframe)])
print(result)

# Items
print("\nTest items()")
result = ''
for worker in client_worker_list:
    result = result + worker.items(Worker_pb2.EmptyWorker()).items + "\n"
print(result)

# Max
print("\nTest max(0)")
result = pd.Series([-sys.maxsize - 1, -sys.maxsize - 1, -sys.maxsize - 1], index=['x', 'y', 'z'])
for worker in client_worker_list:
    max_wk = pickle.loads(worker.max(Worker_pb2.Axis(axis=0)).series)
    i = 0
    for index, value in max_wk.items():
        if value > result[i]:
            result[i] = value
        i += 1
print(result)

# Min
print("\nTest min(0)")
result = pd.Series([sys.maxsize, sys.maxsize, sys.maxsize], index=['x', 'y', 'z'])
for worker in client_worker_list:
    max_wk = pickle.loads(worker.min(Worker_pb2.Axis(axis=0)).series)
    i = 0
    for index, value in max_wk.items():
        if value < result[i]:
            result[i] = value
        i += 1
print(result)

finish = time.time()
print("Execution time: " + str(finish-start))
