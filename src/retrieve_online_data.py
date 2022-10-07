
import pandas as pd
from utils import delete_path, return_path_to, write_df_to_path, read_csv_to_df, return_path_to, check_if_file, get_response
from classes.scrapers import ToSDRRetriever, PrivSpyRetriever, DataRetriever
from classes.text_processors import GeneralTextProcessor
from settings.config import url_map

def scrape_tosdr_topics():
    save_path = return_path_to("/data/saved/tosdr/tosdr_topics.csv")
    print("Scraping ToS;DR topics...")
    df_topics = dr_tosdr.scrape_all_topics()
    write_df_to_path(df_topics,save_path, overwrite=True)
    return df_topics

def scrape_tosdr_cases(topic_ids):
    save_path = return_path_to("/data/saved/tosdr/tosdr_cases.csv")
    print("Scraping ToS;DR cases...")
    df_cases = dr_tosdr.scrape_all_cases(topic_ids)
    write_df_to_path(df_cases,save_path, overwrite=True)
    return df_cases

def scrape_tosdr_points(case_ids):
    save_path = return_path_to("/data/saved/tosdr/tosdr_points.csv")
    print("Scraping ToS;DR points...")
    delete_path(save_path)
    while True:
        try:   
            df_points = dr_tosdr.scrape_all_points(case_ids)
            break
        except Exception as excptn:
            print(excptn)
            print("\n\nRescraping last incomplete cases...\n")
            pass
    write_df_to_path(df_points,save_path, overwrite=True)
    return df_points

def tosdr_case_num_to_str(case_id):
    topic_id = tosdr_cases[tosdr_cases['case_id'] == case_id].iloc[0]['topic']
    case_title = tosdr_cases[tosdr_cases['case_id'] == case_id].iloc[0]['case_title']
    topic_title = tosdr_topics[tosdr_topics['topic_id'] == topic_id].iloc[0]['topic_title']
    return topic_title, case_title

def retrieve_privspy_points(path_source):
    path_storage = return_path_to("/data/saved/privacyspy/privspy_points.csv")
    print("Retrieving PrivacySpy points from {}...".format(path_source))
    df_points = dr_privspy.extract_point_info_from_products(path_source)
    write_df_to_path(df_points,path_storage, overwrite=True)
    return df_points

def privspy_expand_sources(input_df):
    multiple_sources = input_df[input_df['sources'].str.contains(', ')]
    out_df = input_df[~input_df['sources'].str.contains(', ')]
    
    temp_df = pd.DataFrame(columns=multiple_sources.columns)
    i = 0
    for idx, row in multiple_sources.iterrows():
        sources = row.sources.split(", ")
        for source in sources:
            row.sources = source
            temp_df.loc[i] = row
            i = i+1
    
    return pd.concat([out_df, temp_df])

def get_english_doc_urls(points_df, conf_threshold):
    all_urls = list(points_df['source_url'].unique())
    selected_urls = []
    for url in all_urls:
        sub_df = points_df[points_df['source_url'] == url]
        df_en = sub_df[sub_df['lang'] == "en"]
        len_all = len(sub_df)
        len_en = len(df_en)
        if len_en/len_all <= 0.5:
            pass
        elif df_en['prec'].mean() < conf_threshold:
            pass
        else:
            selected_urls.append(url)
    return selected_urls

def map_based_on_dict(inpt, map_dict):
    if inpt in map_dict.keys():
        return map_dict[inpt]
    else:
        return inpt

def url_to_service(url):
    service_title = selected_points[selected_points['source_url'] == url].iloc[0]['service']
    return service_title


# TERMS OF SERVICE; DIDN'T READ
print("#### RETRIEVING: Terms of Service; Did Not Read ####")
path_tosdr = return_path_to("/data/saved/tosdr/")
dr_tosdr = ToSDRRetriever()

## retrieve and save all topics (top level categories)
topic_path = "".join([path_tosdr, "tosdr_topics.csv"])
if not check_if_file(topic_path):
    tosdr_topics = scrape_tosdr_topics()
else:
    resp = get_response("A file containing topics already exists. Do you want to rescrape? (y/n)")
    if resp:
        tosdr_topics = scrape_tosdr_topics()
    else:
        tosdr_topics = read_csv_to_df(topic_path, verbose=True)

## retrieve and save all cases (labels)
cases_path = "".join([path_tosdr, "tosdr_cases.csv"])
if not check_if_file(cases_path):
    tosdr_cases = scrape_tosdr_cases()
else:
    resp = get_response("A file containing cases already exists. Do you want to rescrape? (y/n)")
    if resp:
        tosdr_cases = scrape_tosdr_cases(tosdr_topics['topic_id'])
    else:
        tosdr_cases = read_csv_to_df(cases_path, verbose=True)

## retrieve and save all points (labelled spans)
points_path = "".join([path_tosdr, "tosdr_points.csv"])
if not check_if_file(cases_path):
    tosdr_points = scrape_tosdr_points(tosdr_cases['case_id'])
else:
    resp = get_response("A file containing ToS;DR points already exists. Do you want to rescrape? (y/n)")
    if resp:
        tosdr_points = scrape_tosdr_points(tosdr_cases['case_id'])
    else:
        tosdr_points = read_csv_to_df(points_path, verbose=True)

# PRIVACY SPY
print("\n\n#### RETRIEVING: PrivacySpy ####")
path_source = return_path_to("/data/sources/privacyspy/products/")
path_storage = return_path_to("/data/saved/privacyspy/privspy_points.csv")
dr_privspy = PrivSpyRetriever()

# get all "points" (labelled spans)  based on Privacyspy Repo
if not check_if_file(path_storage):
    privspy_points = retrieve_privspy_points(path_source)
else:
    resp = get_response("A file containing PrivacySpy points already exists. Do you want to rescrape? (y/n)")
    if resp:
        privspy_points = retrieve_privspy_points(path_source)
    else:
        privspy_points = read_csv_to_df(path_storage, verbose=True)

# COMBINING (AND REDUCING) SCRAPED DATA
print("\n\n#### COMBINING INFORMATION ####")
gtp = GeneralTextProcessor()

# add relevant data to data frames
## include only approved points from ToS;DR
print("Preprocessing data frames...")
tosdr_appr = tosdr_points[tosdr_points["point_status"] == "APPROVED"]
## transform numeric information from ToS;DR data
tosdr_appr[["topic", "value"]] = tosdr_appr.apply(lambda row: tosdr_case_num_to_str(row.case_id), axis='columns', result_type='expand')
## expand PrivacySpy data frame in case multiple sources are named
privspy_ext = privspy_expand_sources(privspy_points)

# reduce both data frames to relevant columns
print("Reducing both data frames to relevant columns...")
## tosdr
tosdr = tosdr_appr[["point_text", "service_id", "point_source", "topic", "value"]]
tosdr.insert(5, "source", "tosdr")
tosdr = tosdr.rename(columns={'point_text': "text", "service_id": "service", "point_source": "source_url"})
## privspy
privspy = privspy_ext[["text", "service_name", "sources", "tag","tagval"]]
privspy.insert(5, "source", "privspy")
privspy = privspy.rename(columns={"service_name": "service", "sources": "source_url", "tag":"topic", "tagval":"value"})

# combine data frames
print("Combining data from ToS;DR and PrivacySpy...")
all_points = pd.concat([tosdr,privspy])
# standardise url format
all_points["source_url"] = all_points.apply(lambda row: gtp.clean_url(row.source_url), axis='columns')




print("\n\n#### REDUCING COMBINED DATA ####")
# remove unwanted URLS
print("Removing points with unwanted urls...")
## remove points coming from "groups.google" and "//github.com/" pages (excludes help.github.com)
reduced_points = all_points[~((all_points['source_url'].str.contains('groups.google')) | ((all_points['source_url'].str.contains('github.com')) & ~(all_points['source_url'].str.contains('help.github.com'))))]

# remove NaNs
print("Removing points without text...")
reduced_points = reduced_points[~reduced_points['text'].isna()]

# remove all documents with less than 6 tags
print("Removing documents including less than six points...")
reduced_points = reduced_points.groupby("source_url").filter(lambda group: group.source_url.size >= 6)

# remove non-English documents
print("Removing non-English documents...")
reduced_points[["lang", "prec"]] = reduced_points.apply(lambda row: gtp.guess_language(row.text), axis='columns', result_type='expand')
## remove all where majority of points is not classified as english and average 'certainty' is above 0.6
selected_urls = get_english_doc_urls(reduced_points, 0.6)
## map similar urls to same url
reduced_points["source_url"] = reduced_points.apply(lambda row: map_based_on_dict(row.source_url, url_map), axis='columns')
## include only selected urls
selected_points = reduced_points[reduced_points['source_url'].isin(selected_urls)]

# create overview over documents
documents = pd.DataFrame({'docUID':range(2000,2000+len(selected_points['source_url'].unique())), 'docURL':selected_points['source_url'].unique()})
documents['service'] = documents.apply(lambda row: url_to_service(row.docURL), axis='columns')
documents = documents[['docUID', 'service', 'docURL']]

print("\n\n#### The process has yielded {} points in {} documents. ####".format(len(selected_points), len(documents)))

# save
path_save = return_path_to("/data/saved/combined/")
## points 
points_path = "".join([path_save, "points.csv"])
write_df_to_path(selected_points, points_path)
selected_points = read_csv_to_df(points_path)
## documents
docs_path = "".join([path_save, "docs.csv"])
write_df_to_path(documents, docs_path)
documents = read_csv_to_df(docs_path)

print("\n\n#### RETRIEVING SANITIZED DOCUMENTS ####".format(len(selected_points), len(documents)))
resp = get_response("Do you want to start scraping the corresponding documents? (y/n)")
if resp:
    print("\n\nScraping and saving documents...")
    docs_path = "".join([path_save, "sanitized_docs"])
    dr = DataRetriever()
    dr.scrape_and_save_documents(documents, docs_path, sanitize=True)
else:
    print("\n\nProcess cancelled.")