from genericpath import isfile
import pandas as pd

from json import loads
from os.path import isfile

#from frozendict import frozendict
from datetime import datetime

def freeze_dicts(input_df):
    df = input_df.copy(deep=True)
    for column in df.columns:
        if type(df[column].get(0)) == dict:
            #df[column] = df[column].transform(lambda k: frozendict(k.items()))
            pass
    return df

def flatten(lst):
    return [elem for sublist in lst for elem in sublist]

def read_file(file_path):
    with open(file_path, 'r') as f:
        data = f.read()
    return data

def read_json(file_path):
    with open(file_path) as f:
        res = loads(f.read())
    return res

def read_csv_to_df(file_path):
    return pd.read_csv(file_path)

def write_df_to_path(df,file_path):
    if isfile(file_path):
        resp = input("{} already exists. Do you want to overwrite? (y/n)".format(file_path))
        if resp != "y":
            return
    print("Writing {} to file sytsem.".format(file_path))
    df.to_csv(file_path, index=False)
