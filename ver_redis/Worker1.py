import pickle

import pandas as pd
import redis

# Set up logging
worker = redis.Redis(host='localhost', port=6379, db=1)
pubsub = worker.pubsub()
pubsub.subscribe('methods')

# Set up client to master
master = redis.from_url('redis://localhost:6379', db=0)

df = pd.DataFrame()


def read_csv(url):
    global df
    df = pd.read_csv(url)


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
            method = message['data'].decode("UTF-8").split(";")[0]
            arg = ""
            if len(message['data'].decode("UTF-8").split(";")) == 2:
                arg = message['data'].decode("UTF-8").split(";")[1]
            if method == 'read_csv':
                arg = arg.split(",")[0]
                read_csv(arg)
            elif method == 'apply':
                apply(arg)
            elif method == 'columns':
                columns()
            elif method == 'groupby':
                groupby(arg)
            elif method == 'head':
                head(int(arg))
            elif method == 'isin':
                arg = arg.split(",")
                for i in range(0, len(arg)):
                    arg[i] = int(arg[i])
                isin(arg)
            elif method == 'items':
                items()
            elif method == 'max':
                max(int(arg))
            elif method == 'min':
                min(int(arg))
except KeyboardInterrupt:
    print("Exiting")
    worker.flushdb()
    exit()
