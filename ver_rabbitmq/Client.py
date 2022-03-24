import pickle

import pandas
import pika

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
worker_exchange = channel.exchange_declare(exchange='worker_exchange', exchange_type='fanout')
response_queue = channel.queue_declare(queue='response_queue')

channel.queue_purge(queue='response_queue')

# Read CSV
channel.basic_publish(exchange='worker_exchange', routing_key='', body="read_csv('../df.csv')")

# columns()
channel.basic_publish(exchange='worker_exchange', routing_key='', body="columns()")
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    worker_result = pickle.loads(channel.basic_get(queue='response_queue')[2])
    print(worker_result)

# head()
print("\n--- head(5) test ---")
dataframe = pandas.DataFrame()
channel.basic_publish(exchange='worker_exchange', routing_key='', body="head(5)")
while channel.queue_declare(queue='response_queue').method.message_count > 0:
    dataframe = pandas.concat([dataframe, pickle.loads(channel.basic_get(queue='response_queue')[2])])
print(dataframe)

connection.close()
