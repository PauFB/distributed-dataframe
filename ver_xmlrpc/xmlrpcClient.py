import pickle
import sys
import time
from xmlrpc.client import ServerProxy
import pandas as pd

start = time.time()

# Communication with master
master = ServerProxy('http://localhost:8000', allow_none=True)

# Get workers
workers_list = master.get_workers()
client_worker_list = list()
for worker in workers_list:
    client_worker_list.append(ServerProxy(worker, allow_none=True))

# Read CSV
csv_list = ["../df1.csv", "../df2.csv", "../df3.csv"]

i = 0
for worker in client_worker_list:
    # worker.read_csv(csv_list.__getitem__(i))
    worker.read_csv(csv_list[i])
    i += 1

# Apply
print("\nTest apply(lambda x: x + 2)")
result = pd.DataFrame(columns=['x', 'y', 'z'])
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.apply("lambda x: x + 2").data)])
print(result)

# Columns
print("\nTest columns()")
result = pd.Index([])
for worker in client_worker_list:
    result = result.union(pickle.loads(worker.columns().data))
print(result)

# GroupBy
print("\nTest groupby(z).sum()")
result = pd.DataFrame(columns=['x', 'y'])
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.groupby('z').data).sum()])
print(result)

# Head
print("\nTest head()")
result = pd.DataFrame(columns=['x', 'y', 'z'])
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.head().data)])
print(result)

# isin
print("\nTest isin([2, 4])")
result = pd.DataFrame(columns=['x', 'y', 'z'])
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.isin([2, 4]).data)])
print(result)

# items
print("\nTest items")
result = ''
for worker in client_worker_list:
    result = result + "\n" + worker.items()
print(result)

# max
print("\nTest max(0)")
result = pd.Series([-sys.maxsize - 1, -sys.maxsize - 1, -sys.maxsize - 1], index=['x', 'y', 'z'])
for worker in client_worker_list:
    max_wk = pickle.loads(worker.max(0).data)
    i = 0
    for index, value in max_wk.items():
        if value > result[i]:
            result[i] = value
        i += 1
print(result)

# min
print("\nTest min(0)")
result = pd.Series([sys.maxsize, sys.maxsize, sys.maxsize], index=['x', 'y', 'z'])
for worker in client_worker_list:
    max_wk = pickle.loads(worker.min(0).data)
    i = 0
    for index, value in max_wk.items():
        if value < result[i]:
            result[i] = value
        i += 1
print(result)

finish = time.time()
print("Execution time: " + str(finish-start))

print("\n\n*** Individual Tests ***\n\n")
for worker in client_worker_list:
    print("Test apply(lambda x: x + 2)")
    print(pickle.loads(worker.apply("lambda x: x + 2").data))
    print("Test columns()")
    print(pickle.loads(worker.columns().data))
    print("Test groupby(x).sum()")
    print(pickle.loads(worker.groupby('x').data).sum())
    print("Test head()")
    print(pickle.loads(worker.head().data))
    print("Test isin([2, 4])")
    print(pickle.loads(worker.isin([2, 4]).data))
    print("Test items()")
    print(worker.items())
    print("Test max(0)")
    print(pickle.loads(worker.max(0).data))
    print("Test min(0)")
    print(pickle.loads(worker.min(0).data))
