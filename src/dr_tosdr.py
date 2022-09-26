from classes_general import DataRetriever

from tqdm import tqdm
from copy import deepcopy
from datetime import datetime
from time import sleep
from utils import freeze_dicts

import os
import requests
import pandas as pd

class ToSDR_Retriever(DataRetriever):

    def __init__(self) -> None:
        super().__init__()

    def get_tag_content(self, bs_tag):
        return bs_tag.get('href').split('/')[-1]

    def drop_link_column(self, input_df):
        df = input_df.copy(deep=True)
        if 'links' in df.columns:
            df = df.drop('links',axis=1)
        return df

# TOPICS
    def get_topics_from_html(self):
        topic_soup = self.response_to_soup("https://edit.tosdr.org/topics/")
        return topic_soup.find_all('a',title="View more details")

    def topic_dict_from_tag(self, tag):
        topic_url = "".join(['https://edit.tosdr.org',tag.get('href')])
        res_dict = {'topic_id': int(self.get_tag_content(tag)),'topic_title': tag.contents[0],'topic_url':topic_url}
        return res_dict

    def scrape_topic_info(self, topic_tag):
        res_dict = self.topic_dict_from_tag(topic_tag)
        topic_details = self.response_to_soup(res_dict['topic_url'])
        res_dict['topic_heading'] = topic_details.find('sub').contents[0]
        res_dict['topic_description'] = self.gtp.replace_html_tags(topic_details.find('p').contents[0])
        if res_dict['topic_description'] == "description":
            res_dict['topic_description'] = ""
        
        return res_dict

    def scrape_all_topics(self):
        topic_tag_list = self.get_topics_from_html()
        for idx, topic_tag in tqdm(enumerate(topic_tag_list), total=len(topic_tag_list)):
            topic_dict = self.scrape_topic_info(topic_tag)
            df = pd.DataFrame([topic_dict])
            if idx == 0:
                df_res = deepcopy(df)
            else:
                df_res = pd.concat([df_res, df], ignore_index = True)
        return df_res

# CASES
## API
    def get_case_df_from_api(self, page=None) -> pd.DataFrame:
        if not page:
            response = requests.get("https://api.tosdr.org/case/v1/")
        else:
            
            response = requests.get("https://api.tosdr.org/case/v1//page={page}")
        
        data_cases = response.json()['parameters']['cases']
        df = pd.DataFrame(data_cases)
        df = self.drop_link_column(df)
        df = freeze_dicts(df)
        return df
    
    def query_case_api_pages(self,page_start=1, page_end=10) -> pd.DataFrame:
        df_tosdr = self.get_case_df_from_api(page_start)
        print("page: {}, columns in combined data frame: {}".format(page_start,len(df_tosdr)))
        
        for i in range(page_start+1,page_end+1):
            df_resp = self.get_case_df_from_api(i)
            df_tosdr = pd.concat([df_tosdr, df_resp], ignore_index = True)
            df_tosdr = df_tosdr.drop_duplicates()
            print("page: {}, columns in combined data frame: {}".format(i, len(df_tosdr)))   

        return df_tosdr

## SCRAPER
    def case_dict_from_tag(self,tag,topic_id) -> dict:
        case_url = "".join(['https://edit.tosdr.org',tag.get('href')])
        res_dict = {'case_id': self.get_tag_content(tag),'case_title': tag.contents[0],'topic_id':topic_id, 'case_url':case_url}
        return res_dict
    
    def get_cases_from_topic_tag(self, topic_tag) -> list:
        url = "".join(['https://edit.tosdr.org',topic_tag.get('href')])
        topic_id = int(self.get_tag_content(topic_tag))
        
        topic_details = self.response_to_soup(url)
        case_list = topic_details.find_all('a', title='View points connected to this case')
        case_list = [self.dict_from_tag(case,topic_id) for case in case_list]
        
        return case_list

    def clean_up_case_columns(self, input_df) -> pd.DataFrame:
        case_df = input_df.copy(deep=True)

        def unix_to_utc(x):
            return datetime.utcfromtimestamp(x['unix']).strftime('%Y-%m-%d %H:%M:%S')

        def get_rating_hex(x):
            return x['hex']

        def get_rating_name(x):
            return x['human']

        case_df['updated_at'] = case_df['updated_at'].apply(unix_to_utc)
        case_df['created_at'] = case_df['created_at'].apply(unix_to_utc)
        case_df['rating_hex'] = case_df['classification'].apply(get_rating_hex)
        case_df['rating_name'] = case_df['classification'].apply(get_rating_name)
        del case_df['classification']

        return case_df

    def scrape_case_info(self,case_tag, topic_id):
        res_dict = self.case_dict_from_tag(case_tag, topic_id)
        case_soup = self.response_to_soup(res_dict['case_url'])
        case_details = case_soup.find_all('div', attrs={"class":"col-lg-6"})
        
        res_dict['weight'] = case_details[1].find_all("p")[1].contents[0].split(":")[-1].strip()
        res_dict['rating_name'] = case_details[1].find_all("p")[0].contents[0].split(":")[-1].strip()

        res_dict['case_description'] = ""
        if len(case_details[0].p.contents) != 0:
            res_dict['case_description'] = self.gtp.replace_html_tags(case_details[0].p.contents[0])
        
        return res_dict

    def scrape_all_cases(self,topic_ids):
        for idx1, topic_id in tqdm(enumerate(topic_ids), total=len(topic_ids)):
            topic_soup = self.response_to_soup("https://edit.tosdr.org/topics/{topic_id}".format(topic_id=topic_id))
            case_tag_list = topic_soup.find_all('a',title="View points connected to this case")

            for idx2, case_tag in tqdm(enumerate(case_tag_list), total=len(case_tag_list),leave=False):
                try:
                    case_dict = self.scrape_case_info(case_tag, topic_id)
                except:
                    print("Issues with case {}. Please check manually at https://edit.tosdr.org/topics/{}".format(self.get_tag_content(case_tag),self.get_tag_content(case_tag)))
                df = pd.DataFrame([case_dict])
                if idx1 == 0 and idx2 == 0:
                    df_res = deepcopy(df)
                else:
                    df_res = pd.concat([df_res, df], ignore_index = True)
        return df_res

# POINTS

    def point_dict_from_tag(self,tag,case_id) -> dict:
        point_url = "".join(['https://edit.tosdr.org',tag.get('href')])
        res_dict = {'case_id':case_id,'point_id': self.get_tag_content(tag),'point_url':point_url}
        return res_dict

    def add_point_content(self,point_dict, point_soup):
        text_info = point_soup.find_all('div', attrs={"class":"col-sm-10 col-sm-offset-1 p30 bgw"})

        point_dict["point_text"] = ""
        point_dict["doc_id"] = ""
        point_dict["doc_type"] = ""

        if len(text_info) != 0:
            if len(text_info[0].find_all("blockquote")) != 0:
                point_dict["point_text"] = self.gtp.replace_html_tags(text_info[0].find("blockquote").contents[0].strip())
                point_dict["doc_id"] = text_info[0].find("blockquote").a.get("href").split('#')[-1]
                point_dict["doc_type"] = text_info[0].find("blockquote").a.contents[0]
            elif len(text_info[0].contents[0].strip()) != 0:
                point_dict["point_text"] = text_info[0].contents[0].strip()
        
        return point_dict

    def add_point_info(self,point_dict, point_soup):
        point_details = point_soup.find_all('div', attrs={"class":"col-sm-2"})
        
        point_dict['service_id'] = point_details[0].a.get("href").split('/')[-1]
        point_dict['point_status'] = point_details[1].find_all("span")[-1].contents[0].strip()
        point_dict['point_source'] = point_details[4].a.get("href")

        return point_dict


    def scrape_point_info(self,point_tag, case_id):
        res_dict = self.point_dict_from_tag(point_tag, case_id)

        point_soup = self.response_to_soup(res_dict['point_url'])
        try:
            res_dict = self.add_point_info(res_dict,point_soup)
            res_dict = self.add_point_content(res_dict,point_soup)
        except:
            print("Issues with point {}. Please check manually at https://edit.tosdr.org/points/{}".format(self.get_tag_content(point_tag),self.get_tag_content(point_tag)))
        
        return res_dict


    def scrape_all_points(self,case_ids):
        points_path = "".join([os.getcwd(), "/data/saved/tosdr/tosdr_points.csv"])
        if os.path.isfile(points_path):
            df_res = pd.read_csv(points_path)
            case_id_list = [case for case in case_ids if case not in set(df_res.case_id)]
        else:
            case_id_list = case_ids
        
        for idx1, case_id in tqdm(enumerate(case_id_list), total=len(case_id_list)):
            case_soup = self.response_to_soup("https://edit.tosdr.org/cases/{}".format(case_id))
            point_tag_list = case_soup.find_all('a',title="View more details")
            for idx2, point_tag in tqdm(enumerate(point_tag_list), total=len(point_tag_list),leave=False):
                point_dict = self.scrape_point_info(point_tag, case_id)
                df = pd.DataFrame([point_dict])
                if idx1 == 0 and idx2 == 0 and not os.path.isfile(points_path):
                    df_res = deepcopy(df)
                else:
                    df_res = pd.concat([df_res, df], ignore_index = True)
                sleep(2)
            df_res.to_csv(points_path, index=False)
        return df_res

### DOCUMENTS

    def scrape_doc_info(self, doc_tag, service_id):
        res_dict = {}
        res_dict['service_id'] = service_id
        res_dict['doc_id'] = int(doc_tag.find("a").get("href").split("/")[-1])
        res_dict['doc_url'] = "".join(['https://edit.tosdr.org',doc_tag.find("a").get("href")])
        
        res_dict["doc_type"] = doc_tag.find("a").contents[0]
        res_dict["doc_content"] = self.gtp.html_unescape(doc_tag.find("div", attrs={"class":"documentContent"}))
        
        return res_dict

    def scrape_all_docs(self, service_ids, cookies, headers):
        docs_path = "".join([os.getcwd(), "/data/saved/tosdr/tosdr_docs.csv"])
        if os.path.isfile(docs_path):
            df_res = pd.read_csv(docs_path)
            service_id_list = [service for service in service_ids if service not in set(df_res.service_id)]
        else:
            service_id_list = service_ids
        
        for idx1, service_id in tqdm(enumerate(service_id_list), total=len(service_id_list)):
            service_soup = self.response_to_soup("https://edit.tosdr.org/services/{}/annotate".format(service_id), cookies, headers)
            doc_tag_list = service_soup.find_all('div', attrs={"class":"panel-default"})

            if len(doc_tag_list) > 0:
                for idx2, doc_tag in tqdm(enumerate(doc_tag_list), total=len(doc_tag_list),leave=False):
                    doc_dict = self.scrape_doc_info(doc_tag, service_id)
                    df = pd.DataFrame([doc_dict])
                    if idx1 == 0 and idx2 == 0 and not os.path.isfile(docs_path):
                        df_res = deepcopy(df)
                    else:
                        df_res = pd.concat([df_res, df], ignore_index = True)
                    sleep(2)
                df_res.to_csv(docs_path, index=False)
        return df_res