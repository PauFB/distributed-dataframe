import pickle

import pandas as pd
import redis

master = redis.from_url('redis://localhost:6379', db=0)

client_worker_list = []
i = 0
while i < master.llen("workers"):
    client_worker_list.append(redis.from_url(master.lindex("workers", i).decode("utf-8")))
    i += 1
print(client_worker_list)

# Read CSV
csv_list = ["../resources/df1.csv", "../resources/df2.csv"]

i = 0
for worker in client_worker_list:
    worker.set("df", pickle.dumps(pd.read_csv(csv_list[i])))
    i += 1

# apply
print("\nTest apply(lambda x: x + 2)")
result = pd.DataFrame(columns=['x', 'y', 'z'])
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.get("df")).apply(eval("lambda x: x + 2"))])
print(result)

# columns
print("\nTest columns()")
result = pd.Index([])
for worker in client_worker_list:
    result = result.union(pickle.loads(worker.get("df")).columns)
print(result)

# groupby
print("\nTest groupby(z).sum()")
result = pd.DataFrame(columns=['x', 'y'])
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.get("df")).groupby('z').sum()])
print(result)

# head
print("\nTest head()")
result = pd.DataFrame(columns=['x', 'y', 'z'])
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.get("df")).head()])
print(result)

# isin
print("\nTest isin([2, 4])")
result = pd.DataFrame(columns=['x', 'y', 'z'])
for worker in client_worker_list:
    result = pd.concat([result, pickle.loads(worker.get("df")).isin([2, 4])])
print(result)

# items
print("\nTest items")
for worker in client_worker_list:
    for label, content in pickle.loads(worker.get("df")).items():
        print(f'label: {label}')
        print(f'content: {content}', sep='\n')

# max
print("\nTest max(0)")
result = pd.Series([0, 0, 0], index=['x', 'y', 'z'])
for worker in client_worker_list:
    max_wk = pickle.loads(worker.get("df")).max(0)
    i = 0
    for index, value in max_wk.items():
        if value > result[i]:
            result[i] = value
        i += 1
print(result)

# min
print("\nTest min(0)")
result = pd.Series([999, 999, 999], index=['x', 'y', 'z'])
for worker in client_worker_list:
    max_wk = pickle.loads(worker.get("df")).min(0)
    i = 0
    for index, value in max_wk.items():
        if value < result[i]:
            result[i] = value
        i += 1
print(result)
