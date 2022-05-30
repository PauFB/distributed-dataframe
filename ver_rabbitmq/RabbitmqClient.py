import pickle
import sys
import time
import pandas as pd
import pika

start = time.time()

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
worker_exchange = channel.exchange_declare(exchange='worker_exchange', exchange_type='fanout')
response_queue = channel.queue_declare(queue='response_queue')

channel.queue_purge(queue='response_queue')

# Read CSV
channel.basic_publish(exchange='worker_exchange', routing_key='', body="read_csv('../df.csv')")

# Apply
print("\nTest apply(lambda x: x + 2)")
result = pd.DataFrame(columns=['x', 'y', 'z'])
channel.basic_publish(exchange='worker_exchange', routing_key='', body="apply(\"lambda x: x + 2\")")
while channel.queue_declare(queue='response_queue').method.message_count < 3:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    result = pd.concat([result, pickle.loads(channel.basic_get(queue='response_queue')[2])])
print(result)

# columns()
print("\nTest columns()")
channel.basic_publish(exchange='worker_exchange', routing_key='', body="columns()")
while channel.queue_declare(queue='response_queue').method.message_count < 3:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    worker_result = pickle.loads(channel.basic_get(queue='response_queue')[2])
    print(worker_result)

# GroupBy
print("\nTest groupby(z).sum()")
result = pd.DataFrame(columns=['x', 'y'])
channel.basic_publish(exchange='worker_exchange', routing_key='', body="groupby(\"z\").sum()")
while channel.queue_declare(queue='response_queue').method.message_count < 3:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    result = pd.concat([result, pickle.loads(channel.basic_get(queue='response_queue')[2])])
print(result)

# head()
print("\nTest head(5)")
dataframe = pd.DataFrame()
channel.basic_publish(exchange='worker_exchange', routing_key='', body="head(5)")
while channel.queue_declare(queue='response_queue').method.message_count < 3:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    dataframe = pd.concat([dataframe, pickle.loads(channel.basic_get(queue='response_queue')[2])])
print(dataframe)

# isin
print("\nTest isin([2, 4])")
result = pd.DataFrame(columns=['x', 'y', 'z'])
channel.basic_publish(exchange='worker_exchange', routing_key='', body="isin([2, 4])")
while channel.queue_declare(queue='response_queue').method.message_count < 3:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    result = pd.concat([result, pickle.loads(channel.basic_get(queue='response_queue')[2])])
print(result)

# items
print("\nTest items")
result = ''
channel.basic_publish(exchange='worker_exchange', routing_key='', body="items()")
while channel.queue_declare(queue='response_queue').method.message_count < 3:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    result = result + "\n" + pickle.loads(channel.basic_get(queue='response_queue')[2])
print(result)

# max
print("\nTest max(0)")
result = pd.Series([-sys.maxsize - 1, -sys.maxsize - 1, -sys.maxsize - 1], index=['x', 'y', 'z'])
channel.basic_publish(exchange='worker_exchange', routing_key='', body="max(0)")
while channel.queue_declare(queue='response_queue').method.message_count < 3:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    max_wk = pickle.loads(channel.basic_get(queue='response_queue')[2])
    i = 0
    for index, value in max_wk.items():
        if value > result[i]:
            result[i] = value
        i += 1
print(result)

# min
print("\nTest min(0)")
result = pd.Series([sys.maxsize, sys.maxsize, sys.maxsize], index=['x', 'y', 'z'])
channel.basic_publish(exchange='worker_exchange', routing_key='', body="min(0)")
while channel.queue_declare(queue='response_queue').method.message_count < 3:
    pass
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    max_wk = pickle.loads(channel.basic_get(queue='response_queue')[2])
    i = 0
    for index, value in max_wk.items():
        if value < result[i]:
            result[i] = value
        i += 1
print(result)

finish = time.time()
print("Execution time: " + str(finish-start))

connection.close()
