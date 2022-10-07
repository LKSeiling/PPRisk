import re
import toml
import requests
import pandas as pd

from time import sleep
from tqdm import tqdm
from copy import deepcopy
from bs4 import BeautifulSoup
from datetime import datetime

from classes.text_processors import GeneralTextProcessor, HTML_Cleaner
from utils import get_all_files, read_file, return_path_to, check_if_file, freeze_dicts, str_to_file

from selenium.webdriver import Firefox
from selenium.webdriver.common.by import By


class DataRetriever:

    def __init__(self) -> None:
        self.gtp = GeneralTextProcessor()
        self.cleaner = HTML_Cleaner()

    def response_to_soup(self, url, cookies=None, headers=None):
        resp = requests.get(url)
        if cookies and headers:
            resp = requests.get(url, headers=headers, cookies=cookies)
        return BeautifulSoup(resp.content, 'html.parser')

    def get_source_soup(self, source_url, cookie_close=False):
        driver = Firefox()
        try:
            driver.get(source_url)
            if cookie_close:
                sleep(2)
                self.find_and_click(driver, '//button[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "accept")]')
                self.find_and_click(driver, '//button[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "allow")]') 
                self.find_and_click(driver, '//button[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "agree")]') 
                self.find_and_click(driver, '//button[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "enable")]') 
                self.find_and_click(driver, '//button[contains(translate(text(), "ABCDEFGHIJKLMNOPQRSTUVWXYZ", "abcdefghijklmnopqrstuvwxyz"), "got it")]') 
                sleep(2)
            source_soup = BeautifulSoup(driver.page_source, 'html.parser')
            driver.close()
            driver.quit()
            return source_soup
        except:
            driver.close()
            driver.quit()
            return False
        
    def find_and_click(self, driver, xpath):
        try:
            element = driver.find_element(By.XPATH,xpath)
            element.click()
        except Exception as e:
            pass

    def tidy_text_row(self,input_df) -> pd.DataFrame:
        red_df = self.remove_empty_rows(input_df)
        red_df.loc[:,"text"] = red_df.loc[:,"text"].apply(self.gtp.clean_input)
        return red_df

    def get_body_from_url(self, url, cookie_close) -> str:
        source_soup = self.get_source_soup(url, cookie_close=cookie_close)
        return str(source_soup.find("body"))
        

    # reformulate for general body extraction
    def scrape_and_save_documents(self, document_df, save_path, sanitize=False) -> None: 
        problem_urls = []  
        for idx, row in tqdm(document_df.iterrows(), total=len(document_df)):
            filename = "{}/{}_{}.html".format(save_path,row.docUID, row.service)
            url = "".join(["https://www.",row.docURL])
            try:
                body_str = self.get_body_from_url(url, cookie_close=True)
                if sanitize:
                    body_str = self.cleaner.html_to_sanitized(body_str)
                str_to_file(body_str, filename)
            except:
                problem_urls.append(url)
        if len(problem_urls) > 0:
            print("\nIssues occured while trying to scrape the following URLs:")
            for url in problem_urls:
                print(url)
            print("\nPlease check manually.")
                



class ToSDRRetriever(DataRetriever):

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
        text_info = point_soup.findl('div', attrs={"class":"col-sm-10 col-sm-offset-1 p30 bgw"})

        point_dict["point_text"] = ""
        point_dict["doc_id"] = ""
        point_dict["doc_type"] = ""

        if len(text_info) != 0:
            if len(text_info[0].find_all("blockquote")) != 0:
                point_dict["doc_id"] = text_info[0].find("blockquote").a.get("href").split('#')[-1]
                point_dict["doc_type"] = text_info[0].find("blockquote").a.contents[0]
                point_dict["point_text"] = self.extract_point_text(text_info)

            elif len(text_info[0].contents[0].strip()) != 0:
                point_dict["point_text"] = text_info[0].contents[0].strip()

        return point_dict

    def extract_point_text(self, text_info):
        text = ""
        for bq in text_info.find_all("blockquote"):
            for f in bq.find_all(["footer","span"]):
                f.decompose()
            for t in bq.find_all(string=True):
                if t.strip() != "":
                    text = "".join([text,t.strip()])
        text = text.replace("&nbsp;", " ")
        text = text.replace("\n", " ")
        return text


    def add_point_info(self,point_dict, point_soup):
        point_details = point_soup.find_all('div', attrs={"class":"col-sm-2"})
        
        point_dict['service'] = point_details[0].a.contents[0]
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
        points_path = return_path_to("/data/saved/tosdr/tosdr_points.csv")
        if check_if_file(points_path):
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
                if idx1 == 0 and idx2 == 0 and not check_if_file(points_path):
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
        docs_path = return_path_to("/data/saved/tosdr/tosdr_docs.csv")
        if check_if_file(docs_path):
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
                    if idx1 == 0 and idx2 == 0 and not check_if_file(docs_path):
                        df_res = deepcopy(df)
                    else:
                        df_res = pd.concat([df_res, df], ignore_index = True)
                    sleep(2)
                df_res.to_csv(docs_path, index=False)
        return df_res



class PrivSpyRetriever(DataRetriever):

    def __init__(self) -> None:
        super().__init__()

    def remove_md_links(self,input_str) -> str:
        re_detect = r'\[([\d\w\s]*)\]\(http[s]?:\/\/(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+\)'
        return re.sub(re_detect,r'\1', input_str)

    def clean_toml_multiline(self,data_str) -> str:
        # identify """-sections
        split_list = data_str.split('"""')
        split_list = [elem.strip() for elem in split_list]
        if len(split_list) == 1:
            return split_list[0]
        else:
            # replace \n within """ """-blocks with \\n\\n
            output_content = []
            for idx, elem in enumerate(split_list):
                if idx%2!=0:
                    elem = elem.replace("\n","\\n")
                    elem = elem.replace('"',"'")
                output_content.append(elem)
            return '"'.join(output_content)

    def clean_toml(self,input_str) -> str:
        output_str = self.remove_md_links(input_str)
        output_str = self.clean_toml_multiline(output_str)
        return output_str

    def toml_to_df(self, toml_dict) -> pd.DataFrame:
        text_df = self.extract_quote_info(toml_dict['rubric'])
        text_df.insert(0, "sources", ", ".join(toml_dict['sources']))
        text_df.insert(0, "service_name", toml_dict['name'])
        return text_df

    def extract_quote_info(self, rubric_dict) -> pd.DataFrame:
        res_df = pd.DataFrame(columns=["text","tag","value","tagval","notes", "lang", "prec"])
        for tag, val in rubric_dict.items():
            if 'citations' in val:
                value = val['value']
                tagval = "-".join([tag, value])
                notes = " ".join(val['notes']) if "notes" in val else ""   
                for quote in val['citations']:
                    lang, prec = self.gtp.guess_language(quote)
                    new_row = pd.DataFrame.from_dict({'text':[quote],'tag':[tag],'value':[value], 'tagval':[tagval],'notes':[notes],
                            "lang":[lang], "prec":[prec]})
                    res_df = pd.concat([res_df, new_row], ignore_index = True)
        return res_df

    def extract_point_info_from_products(self, source_path):
        res_df = pd.DataFrame(columns=["service_name", "sources", "text","tag","value","tagval","notes", "lang", "prec"])
        product_list = get_all_files(source_path)
        for product in tqdm(product_list):
            if ".toml" in product:
                product_path = "".join([source_path, product])
                toml_str = self.clean_toml(read_file(product_path))
                toml_dict = toml.loads(toml_str)
                if "rubric" in toml_dict.keys():
                    temp_df = self.toml_to_df(toml_dict)
                    res_df = pd.concat([res_df, temp_df], ignore_index = True)
        return res_df

    def split_sources(self, sources_list):
        all_sources = []
        for elem in sources_list:
            split_elem = elem.split(", ")
            if len(split_elem) == 1:
                all_sources.append(split_elem[0])
            else:
                for sub_elem in split_elem:
                    all_sources.append(sub_elem)
        all_sources = ["".join(["https://www.",source]) if "http" not in source else source for source in all_sources]
        return all_sources