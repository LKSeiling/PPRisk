from classes_general import DataRetriever

import re
import toml
from copy import deepcopy
import pandas as pd
from tqdm import tqdm
from utils import get_all_files, read_file, return_path_to, check_if_file 

from selenium.webdriver import Firefox
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from time import sleep

class PrivSpy_Retriever(DataRetriever):

    def __init__(self) -> None:
        super().__init__()


### READ TOMLS
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

### SCRAPE DOCUMENTS   

    def scrape_source_docs(self, source_list) -> None:
        docs_path = return_path_to("/data/saved/privacyspy/privspy_docs.csv")
        if check_if_file(docs_path):
            df_res = pd.read_csv(docs_path)
            sources = [source for source in source_list if source not in set(df_res.doc_url)]
        else:
            sources = source_list
        
        for idx, source in tqdm(enumerate(sources), total=len(sources)):
            source_soup = self.get_source_soup(source, cookie_close=True)
            if source_soup == False:
                df = pd.DataFrame({"doc_url":[source], "doc_content":"Website Missing"})
            else:
                df = pd.DataFrame({"doc_url":[source], "doc_content":[str(source_soup.find("body"))]})

            if not check_if_file(docs_path):
                df_res = deepcopy(df)
            else:
                df_res = pd.concat([df_res, df], ignore_index = True)
            df_res.to_csv(docs_path, index=False)

        return df_res

