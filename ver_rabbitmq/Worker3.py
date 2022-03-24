import pickle

import pandas
import pika

df = pandas.DataFrame()

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
channel.exchange_declare(exchange='worker_exchange', exchange_type='fanout')
channel.queue_declare(queue='response_queue')
result = channel.queue_declare(queue='', exclusive=True)
queue_name = result.method.queue
channel.queue_bind(exchange='worker_exchange', queue=queue_name)


def read_csv(url):
    global df
    df = pandas.read_csv(url, skiprows=[i for i in range(1, 9)], engine='python')


def apply(func):
    return df.apply(eval(func))


def columns():
    return df.columns


def groupby(by):
    return df.groupby(by)


def head(n=5):
    return df.head(n)


def isin(values):
    return df.isin(values)


def items():
    aux = ''
    for label, content in df.items():
        aux += f'label: {label}\n'
        aux += f'content:\n'
        for c in content:
            aux += f'{c}\n'
    return aux


def max(axis):
    return df.max(axis)


def min(axis):
    return df.min(axis)


def callback(ch, method, properties, body):
    response = pickle.dumps(eval(body))
    channel.basic_publish(exchange='', routing_key='response_queue', body=response)


channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()
