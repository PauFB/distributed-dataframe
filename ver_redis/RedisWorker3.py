import pickle
import redis
import pandas as pd

# Set up logging
worker = redis.Redis(host='localhost', port=6379, db=3)
pubsub = worker.pubsub()
pubsub.subscribe('methods')

# Set up client to master
master = redis.from_url('redis://localhost:6379', db=0)

df = pd.DataFrame()


def read_csv(url):
    global df
    df = pd.read_csv(url, skiprows=[i for i in range(1, 9)])


def apply(func):
    master.rpush("results", pickle.dumps(df.apply(eval(func))))


def columns():
    master.rpush("results", pickle.dumps(df.columns))


def groupby(by):
    master.rpush("results", pickle.dumps(df.groupby(by)))


def head(n=5):
    master.rpush("results", pickle.dumps(df.head(n)))


def isin(values):
    master.rpush("results", pickle.dumps(df.isin(values)))


def items():
    """ list de tuples [label, contentSeries] """
    aux = ''
    for label, content in df.items():
        aux += f'label: {label}\n'
        aux += f'content:\n'
        for c in content:
            aux += f'{c}\n'
    master.rpush("results", pickle.dumps(aux))


def max(axis):
    master.rpush("results", pickle.dumps(df.max(axis)))


def min(axis):
    master.rpush("results", pickle.dumps(df.min(axis)))


try:
    print("Use Ctrl+c to exit")
    for message in pubsub.listen():
        if message['type'] != 'subscribe':
            eval(message['data'])
except KeyboardInterrupt:
    print("Exiting")
    worker.flushdb()
    exit()
