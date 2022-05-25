from dr_privspy import PrivSpy_Retriever
from os import getcwd
import pandas as pd
from tqdm import tqdm

path_all = "".join([getcwd(),"/data/saved/privacyspy/privspy_all_points.csv"])
all_dfs = pd.read_csv(path_all, sep="\t")

sources = list(all_dfs.sources.unique())
all_sources = []
for elem in sources:
    split_elem = elem.split(", ")
    if len(split_elem) == 1:
        all_sources.append(split_elem[0])
    else:
        for sub_elem in split_elem:
            all_sources.append(sub_elem)

all_sources = ["".join(["https://www.",source]) if "http" not in source else source for source in all_sources]

dr_privspy = PrivSpy_Retriever()
dr_privspy.scrape_sources(all_sources)