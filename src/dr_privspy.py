from classes_general import DataRetriever

import re
import pandas as pd
from tqdm import tqdm
from os import getcwd

class PrivSpy_Retriever(DataRetriever):

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

    def clean_toml(self,filepath) -> str:
        with open(filepath, "r") as f:
            data_str = f.read()
            f.close
        data_str = self.remove_md_links(data_str)
        data_str = self.clean_toml_multiline(data_str)
        return data_str

    def toml_to_df(self, toml_dict) -> pd.DataFrame:
        text_df = self.extract_citations(toml_dict['rubric'])
        text_df.insert(0, "sources", ", ".join(toml_dict['sources']))
        text_df.insert(0, "service_name", toml_dict['name'])
        return text_df

    def extract_citations(self, rubric_dict) -> pd.DataFrame:
        res_df = pd.DataFrame(columns=["text","tag","value","tagval","notes", "lang", "prec"])
        for tag, val in rubric_dict.items():
            if 'citations' in val:
                value = val['value']
                tagval = "-".join([tag, value])
                notes = " ".join(val['notes']) if "notes" in val else ""   
                for citation in val['citations']:
                    sent_list = self.gtp.split_into_clean_sentences(citation)
                    for sentence in sent_list:
                        if len(sentence.split(" ")) > 3: # mehr als drei wörter
                            lang, prec = self.gtp.guess_language(sentence)
                            new_row = {'text':sentence,'tag':tag,'value':value, 'tagval':tagval,'notes':notes,
                                    "lang":lang, "prec":prec}
                            res_df = res_df.append(new_row,ignore_index=True)
        return res_df

   
    def scrape_sources(self, source_list) -> None:
        res_df = pd.DataFrame(columns=["source", "text","html-tag", "lang", "prec"])
        
        for source in tqdm(source_list):
            try:
                source_soup = self.get_source_soup(source)
                clean_list = self.extract_html_data(source_soup) 
                res_df = pd.DataFrame(clean_list)
                res_df.insert(0, "source", source)
                service_name = source.split(".")[0].split("//")[-1]
                res_df.to_csv("".join([getcwd(),"/data/saved/privacyspy/scraped_policies/",service_name,".csv"]), index=False, sep="\t")
            except:
                pass

            
            