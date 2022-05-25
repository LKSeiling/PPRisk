import re
import requests
import fasttext

from nltk import tokenize
from bs4 import BeautifulSoup


from selenium.webdriver import Firefox
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

class GeneralTextProcessor:

    def __init__(self) -> None:
        PRETRAINED_MODEL_PATH = './models/fasttext/lid.176.bin'
        self.ft_model = fasttext.load_model(PRETRAINED_MODEL_PATH)

    def replace_html_tags(self, input_string, replacement="") -> str:
        return re.sub('<[^>]*>', replacement, input_string)

    def remove_nonalphanumeric(self, input_str) -> str:
        return re.sub(r"(?![)(-:'@,. \w\/])\W{1,}(?<![)(-:'@,. \w\/])", "", input_str)

    def remove_links(self, input_string) -> str:
        return re.sub(r"<.?link[^>]*>|<a[^>]*>", "", input_string)
    
    def remove_md_links(self, input_str) -> str:
        re_detect = r'\[([\d\w\s]*)\]\(http[s]?:\/\/(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+\)'
        return re.sub(re_detect,r'\1', input_str)

    def tknz_sent(self, input_str) -> list:
        return tokenize.sent_tokenize(input_str)

    def to_lower(self, input_str) -> str:
        return input_str.lower()

    def strip(self, input_str) -> str:
        out_string = re.sub(r'(\.\\n)\w',r'\1\. ', input_str)
        out_string = out_string.replace("\n","")
        out_string = out_string.replace("  "," ")
        return out_string.strip()

    def remove_non_character_elems(self, input_list) -> list:
        return [elem for elem in input_list if re.search(r'.*\w.*', elem)]

    def split_into_clean_sentences(self,input_str) -> list:
        sent_list = self.tknz_sent(input_str)
        sent_list = [self.remove_nonalphanumeric(tkn) for tkn in sent_list]
        sent_list = [self.strip(self.to_lower(tkn)) for tkn in sent_list]
        return sent_list

    def return_substr_location(self,sub_str,total_str):
        i1 = total_str.find(sub_str)
        if i1 >= 0:
            return (i1, i1+len(sub_str))
        else:
            return(i1,i1)

    def guess_language(self, inpt) -> tuple:
        label,prec = self.ft_model.predict(inpt)
        lang = label[0].split("__")[2]
        prec_f = prec[0]
        return lang, prec_f



class DataRetriever:

    def __init__(self) -> None:
        self.gtp = GeneralTextProcessor()

    def response_to_soup(self, url, cookies=None, headers=None):
        resp = requests.get(url)
        if cookies and headers:
            resp = requests.get(url, headers=headers, cookies=cookies)
        return BeautifulSoup(resp.content, 'html.parser')

    def get_source_soup(self, source_url):
        driver = Firefox()
        driver.get(source_url)
        source_soup = BeautifulSoup(driver.page_source, 'html.parser')
        driver.close()
        driver.quit()
        return source_soup

    def str_to_file(self, input_str, filepath):
        with open(filepath, "w") as f:
            f.write(input_str)
            f.close

    def extract_html_data(self, base_soup, additional_tags=None) -> list:
        tags_to_keep = ["h1", "h2", "h3", "h4", "p", "li", "b"]
        if additional_tags:
            tags_to_keep = tags_to_keep + additional_tags
        
        res_list = []
        children = base_soup.findChildren(recursive=True)
        for child in children:
            if child.name in tags_to_keep:
                for sentence in self.gtp.split_into_clean_sentences(child.get_text()):
                    # todo MA: via child.find_all_previous() Element verorten
                    lang, prec = self.gtp.guess_language(sentence)
                    res_list.append({'text': sentence, "html-tag": child.name, "lang":lang, "prec":prec})
        return res_list
