import pickle

import pandas as pd

df = pd.DataFrame()


def read_csv(url):
    global df
    df = pd.read_csv(url)


def max(axis):
    return pickle.dumps(df.max(axis))


def apply(func):
    return pickle.dumps(df.apply(eval(func)))


def columns():
    return pickle.dumps(df.columns)


def groupby(by):
    return pickle.dumps(df.groupby(by))


def head(n=5):
    return pickle.dumps(df.head(n))


def isin(values):
    return pickle.dumps(df.isin(values))


def items():
    aux = ''
    for label, content in df.items():
        aux += f'label: {label}\n'
        aux += f'content:\n'
        for c in content:
            aux += f'{c}\n'
    return aux


def max(axis):
    return pickle.dumps(df.max(axis))


def min(axis):
    return pickle.dumps(df.min(axis))
