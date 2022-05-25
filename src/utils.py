import re
import pandas as pd

from bs4 import BeautifulSoup

from frozendict import frozendict
from datetime import datetime

def freeze_dicts(input_df):
    df = input_df.copy(deep=True)
    for column in df.columns:
        if type(df[column].get(0)) == dict:
            df[column] = df[column].transform(lambda k: frozendict(k.items()))
    return df

def flatten(lst):
    return [elem for sublist in lst for elem in sublist]

def read_file(file_path):
    with open(file_path, 'r') as f:
        data = f.read()
        f.close()
    return data