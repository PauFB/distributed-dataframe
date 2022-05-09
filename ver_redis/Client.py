import pickle
import redis
import pandas as pd

master = redis.from_url('redis://localhost:6379', db=0)
n_workers = master.pubsub_numsub("methods")[0][1]

# Read csv
master.publish("methods", "read_csv('../df.csv')")

# Apply
print("\nTest apply(lambda x: x + 2)")

master.publish("methods", "apply('lambda x: x + 2')")

n_results = master.llen("results")
while n_results != n_workers:
    n_results = master.llen("results")

result = pd.DataFrame(columns=['x', 'y', 'z'])
for i in range(n_results):
    result = pd.concat([result, pickle.loads(master.lpop("results"))])
print(result)


# Columns
print("\nTest columns()")

master.publish("methods", "columns()")

n_results = master.llen("results")
while n_results != n_workers:
    n_results = master.llen("results")

result = pd.Index([])
for i in range(n_results):
    result = result.union(pickle.loads(master.lpop("results")))
print(result)


# GroupBy
print("\nTest groupby(z).sum()")

master.publish("methods", "groupby('z')")

n_results = master.llen("results")
while n_results != n_workers:
    n_results = master.llen("results")

result = pd.DataFrame(columns=['x', 'y'])
for i in range(n_results):
    result = pd.concat([result, pickle.loads(master.lpop("results")).sum()])
print(result)


# Head
print("\nTest head(2)")

master.publish("methods", "head(2)")

n_results = master.llen("results")
while n_results != n_workers:
    n_results = master.llen("results")

result = pd.DataFrame(columns=['x', 'y', 'z'])
for i in range(n_results):
    result = pd.concat([result, pickle.loads(master.lpop("results"))])
print(result)


# Isin
print("\nTest isin([2, 4])")

master.publish("methods", "isin([2, 4])")

n_results = master.llen("results")
while n_results != n_workers:
    n_results = master.llen("results")

result = pd.DataFrame(columns=['x', 'y', 'z'])
for i in range(n_results):
    result = pd.concat([result, pickle.loads(master.lpop("results"))])
print(result)


# Items
print("\nTest items()")

master.publish("methods", "items()")

n_results = master.llen("results")
while n_results != n_workers:
    n_results = master.llen("results")

result = ''
for i in range(n_results):
    result = result + pickle.loads(master.lpop("results")) + "\n"
print(result)


# Max
print("\nTest max(0)")

master.publish("methods", "max(0)")

n_results = master.llen("results")
while n_results != n_workers:
    n_results = master.llen("results")

result = pd.Series([0, 0, 0], index=['x', 'y', 'z'])
for i in range(n_results):
    max_wk = pickle.loads(master.lpop("results"))
    j = 0
    for index, value in max_wk.items():
        if value > result[j]:
            result[j] = value
        j += 1
print(result)


# Min
print("\nTest min(0)")

master.publish("methods", "min(0)")

n_results = master.llen("results")
while n_results != n_workers:
    n_results = master.llen("results")

result = pd.Series([999, 999, 999], index=['x', 'y', 'z'])
for i in range(n_results):
    max_wk = pickle.loads(master.lpop("results"))
    j = 0
    for index, value in max_wk.items():
        if value < result[j]:
            result[j] = value
        j += 1
print(result)
