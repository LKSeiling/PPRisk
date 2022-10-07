from genericpath import isfile
import pandas as pd

from json import loads
from os import getcwd, listdir, remove
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

def str_to_file(input_str, filepath):
    with open(filepath, "w") as f:
        f.write(input_str)

def read_file(file_path):
    with open(file_path, 'r') as f:
        data = f.read()
    return data

def read_json(file_path):
    with open(file_path) as f:
        res = loads(f.read())
    return res

def read_csv_to_df(file_path, verbose=False):
    if verbose:
        print("Reading from {}...".format(file_path))
    return pd.read_csv(file_path)

def check_if_file(file_path):
    return isfile(file_path)

def return_path_to(file_path):
    return "".join([getcwd(),file_path])

def delete_path(file_path):
    remove(file_path)

def get_all_files(file_path):
    return listdir(file_path)

def get_response(input_str):
    resp = ""
    edited_str = "".join(["\n", input_str, "   "])
    while resp not in ["y","n"]:
        resp = input(edited_str).lower()
        if resp not in ["y","n"]:
            print("Please respond with either 'y' (yes) or 'n' (no).")
    if resp == "y":
        return True
    elif resp == "n":
        return False

def write_df_to_path(df,file_path, overwrite=False):
    if not overwrite and isfile(file_path):
        resp = input("{} already exists. Do you want to overwrite? (y/n)".format(file_path))
        if resp != "y":
            return
    print("Writing {} to file sytsem.".format(file_path))
    df.to_csv(file_path, index=False)

def remove_empty_rows(self,input_df) -> pd.DataFrame:
    return input_df.loc[~input_df.text.apply(self.gtp.check_if_text_empty),:]