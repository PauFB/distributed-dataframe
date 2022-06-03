import pickle
import sys
import time
from xmlrpc.client import ServerProxy

import pandas as pd

absolute_start_time = time.time()

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
    worker.read_csv(csv_list[i])
    i += 1

# apply()
print("\nTest apply(lambda x: x + 2)")
result = pd.DataFrame(columns=['x', 'y', 'z'])
start = time.time()
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.apply("lambda x: x + 2").data)])
end = time.time()
print(result)
print("Execution time: " + str(end - start))

# columns()
print("\nTest columns()")
result = pd.Index([])
start = time.time()
for worker in client_worker_list:
    result = result.union(pickle.loads(worker.columns().data))
end = time.time()
print(result)
print("Execution time: " + str(end - start))

# GroupBy
print("\nTest groupby(z).sum()")
result = pd.DataFrame(columns=['x', 'y'])
start = time.time()
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.groupby('z').data).sum()])
end = time.time()
print(result)
print("Execution time: " + str(end - start))

# head()
print("\nTest head()")
result = pd.DataFrame(columns=['x', 'y', 'z'])
start = time.time()
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.head().data)])
end = time.time()
print(result)
print("Execution time: " + str(end - start))

# isin()
print("\nTest isin([2, 4])")
result = pd.DataFrame(columns=['x', 'y', 'z'])
start = time.time()
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.isin([2, 4]).data)])
end = time.time()
print(result)
print("Execution time: " + str(end - start))

# items()
print("\nTest items")
result = ''
start = time.time()
for worker in client_worker_list:
    result = result + "\n" + worker.items()
end = time.time()
print(result)
print("Execution time: " + str(end - start))

# max()
print("\nTest max(0)")
result = pd.Series([-sys.maxsize - 1, -sys.maxsize - 1, -sys.maxsize - 1], index=['x', 'y', 'z'])
start = time.time()
for worker in client_worker_list:
    max_wk = pickle.loads(worker.max(0).data)
    i = 0
    for index, value in max_wk.items():
        if value > result[i]:
            result[i] = value
        i += 1
end = time.time()
print(result)
print("Execution time: " + str(end - start))

# min()
print("\nTest min(0)")
result = pd.Series([sys.maxsize, sys.maxsize, sys.maxsize], index=['x', 'y', 'z'])
start = time.time()
for worker in client_worker_list:
    max_wk = pickle.loads(worker.min(0).data)
    i = 0
    for index, value in max_wk.items():
        if value < result[i]:
            result[i] = value
        i += 1
end = time.time()
print(result)
print("Execution time: " + str(end - start))

absolute_end_time = time.time()
print("\nTotal execution time: " + str(absolute_end_time - absolute_start_time))

print("\n\n*** Individual tests ***\n\n")
for worker in client_worker_list:
    print("Testing apply(lambda x: x + 2)")
    print(pickle.loads(worker.apply("lambda x: x + 2").data))
    print("Testing columns()")
    print(pickle.loads(worker.columns().data))
    print("Testing groupby(x).sum()")
    print(pickle.loads(worker.groupby('x').data).sum())
    print("Testing head()")
    print(pickle.loads(worker.head().data))
    print("Testing isin([2, 4])")
    print(pickle.loads(worker.isin([2, 4]).data))
    print("Testing items()")
    print(worker.items())
    print("Testing max(0)")
    print(pickle.loads(worker.max(0).data))
    print("Testing min(0)")
    print(pickle.loads(worker.min(0).data))
