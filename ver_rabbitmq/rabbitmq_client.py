import pickle
import sys
import time

import pandas as pd
import pika

absolute_start_time = time.time()

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
worker_exchange = channel.exchange_declare(exchange='worker_exchange', exchange_type='fanout')
response_queue = channel.queue_declare(queue='response_queue')

channel.queue_purge(queue='response_queue')

# stream = os.popen('curl -i -u guest:guest http://localhost:15672/api/exchanges/%2F/worker_exchange/bindings/source')
# output = stream.read()
# n_workers = output.count('source')
n_workers = 3

# Read CSV
channel.basic_publish(exchange='worker_exchange', routing_key='', body="read_csv('../df.csv')")

# apply()
print("\nTesting apply(lambda x: x + 2)")
result = pd.DataFrame(columns=['x', 'y', 'z'])
start = time.time()
channel.basic_publish(exchange='worker_exchange', routing_key='', body="apply(\"lambda x: x + 2\")")
while channel.queue_declare(queue='response_queue').method.message_count != n_workers:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    result = pd.concat([result, pickle.loads(channel.basic_get(queue='response_queue')[2])])
end = time.time()
print(result)
print("Execution time: " + str(end - start))

# columns()
print("\nTesting columns()")
result = pd.Index([])
start = time.time()
channel.basic_publish(exchange='worker_exchange', routing_key='', body="columns()")
while channel.queue_declare(queue='response_queue').method.message_count != n_workers:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    result = result.union(pickle.loads(channel.basic_get(queue='response_queue')[2]))
end = time.time()
print(result)
print("Execution time: " + str(end - start))

# groupby()
print("\nTesting groupby(z).sum()")
result = pd.DataFrame(columns=['x', 'y'])
start = time.time()
channel.basic_publish(exchange='worker_exchange', routing_key='', body="groupby(\"z\").sum()")
while channel.queue_declare(queue='response_queue').method.message_count != n_workers:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    result = pd.concat([result, pickle.loads(channel.basic_get(queue='response_queue')[2])])
end = time.time()
print(result)
print("Execution time: " + str(end - start))

# head()
print("\nTesting head(5)")
dataframe = pd.DataFrame()
start = time.time()
channel.basic_publish(exchange='worker_exchange', routing_key='', body="head(5)")
while channel.queue_declare(queue='response_queue').method.message_count != n_workers:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    dataframe = pd.concat([dataframe, pickle.loads(channel.basic_get(queue='response_queue')[2])])
end = time.time()
print(dataframe)
print("Execution time: " + str(end - start))

# isin()
print("\nTesting isin([2, 4])")
result = pd.DataFrame(columns=['x', 'y', 'z'])
start = time.time()
channel.basic_publish(exchange='worker_exchange', routing_key='', body="isin([2, 4])")
while channel.queue_declare(queue='response_queue').method.message_count != n_workers:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    result = pd.concat([result, pickle.loads(channel.basic_get(queue='response_queue')[2])])
end = time.time()
print(result)
print("Execution time: " + str(end - start))

# items()
print("\nTesting items()")
result = ''
start = time.time()
channel.basic_publish(exchange='worker_exchange', routing_key='', body="items()")
while channel.queue_declare(queue='response_queue').method.message_count != n_workers:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    result = result + "\n" + pickle.loads(channel.basic_get(queue='response_queue')[2])
end = time.time()
print(result)
print("Execution time: " + str(end - start))

# max()
print("\nTesting max(0)")
result = pd.Series([-sys.maxsize - 1, -sys.maxsize - 1, -sys.maxsize - 1], index=['x', 'y', 'z'])
start = time.time()
channel.basic_publish(exchange='worker_exchange', routing_key='', body="max(0)")
while channel.queue_declare(queue='response_queue').method.message_count != n_workers:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    max_wk = pickle.loads(channel.basic_get(queue='response_queue')[2])
    i = 0
    for index, value in max_wk.items():
        if value > result[i]:
            result[i] = value
        i += 1
end = time.time()
print(result)
print("Execution time: " + str(end - start))

# min()
print("\nTesting min(0)")
result = pd.Series([sys.maxsize, sys.maxsize, sys.maxsize], index=['x', 'y', 'z'])
start = time.time()
channel.basic_publish(exchange='worker_exchange', routing_key='', body="min(0)")
while channel.queue_declare(queue='response_queue').method.message_count != n_workers:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    max_wk = pickle.loads(channel.basic_get(queue='response_queue')[2])
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

connection.close()
