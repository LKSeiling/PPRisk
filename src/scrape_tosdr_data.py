import imp
from dr_tosdr import ToSDR_Retriever
from os import getcwd
from utils import write_df_to_path, read_csv_to_df
from config import cookies, headers

path_all = "".join([getcwd(),"/data/saved/tosdr/"])
dr_tosdr = ToSDR_Retriever()

print("#### RETRIEVING: Terms of Service; Did Not Read ####")
# retrieve and save all topics (top level categories)
print("Retrieving ToS;DR topics.")
#df_topics = dr_tosdr.scrape_all_topics()
#write_df_to_path(df_topics,"".join([path_all,"tosdr_topics.csv"]))

# you might want to load existing data inde
df_topics = read_csv_to_df("".join([path_all,"tosdr_topics.csv"]))

# retrieve and save all cases (labels)
print("\nRetrieving ToS;DR cases.")
#df_cases = dr_tosdr.scrape_all_cases(df_topics['topic_id'])
#write_df_to_path(df_cases,"".join([path_all,"tosdr_cases.csv"]))
df_cases = read_csv_to_df("".join([path_all,"tosdr_cases.csv"]))

# retrieve and save all points (isolated, suggested spans)
print("\nRetrieving ToS;DR points.")
#while True:
#    try:
#        df_points = dr_tosdr.scrape_all_points(df_cases['case_id'])
#        break
#    except Exception as excptn:
#        print(excptn)
#        print("Rescraping last incomplete case...")
#        pass
df_points = read_csv_to_df("".join([path_all, "tosdr_points.csv"]))

# retrieve all documents (including labelled passages)
service_ids = list(set(df_points[df_points['service_id'].isnull() == False]['service_id']))

while True:
    try:
        df_docs = dr_tosdr.scrape_all_docs(service_ids, cookies, headers)
        break
    except Exception as excptn:
        print(excptn)
        print("Rescraping last incomplete documents...")
        pass