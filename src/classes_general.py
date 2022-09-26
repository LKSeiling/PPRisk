from cgitb import text
import re
from time import sleep
import requests
import fasttext
import pandas as pd

from os import getcwd
from html import unescape
from utils import flatten

from nltk import tokenize
from bs4 import BeautifulSoup

from selenium.webdriver import Firefox
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

class GeneralTextProcessor:

    def __init__(self) -> None:
        PRETRAINED_MODEL_PATH = "".join([getcwd(),'/models/fasttext/lid.176.bin'])
        self.ft_model = fasttext.load_model(PRETRAINED_MODEL_PATH)

    def replace_html_tags(self, input_string, replacement="") -> str:
        return re.sub('<[^>]*>', replacement, input_string)

    def remove_nonalphanumeric(self, input_str) -> str:
        return re.sub(r"(?![)(-:&'@,.?! \w\/])\W{1,}(?<![)(-:&'@,.?! \w\/])", "", input_str)

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

    def html_unescape(self, input_str) -> str:
        return unescape(str(input_str))

    def clean_input(self,input_str) -> list:
        out_str = self.remove_nonalphanumeric(self.replace_html_tags(input_str))
        out_str = self.strip(self.to_lower(out_str))
        return out_str

    def remove_non_character_elems(self, input_list) -> list:
        return [elem for elem in input_list if re.search(r'.*\w.*', elem)]

    def check_if_text_empty(self, input_str) -> bool:
        return self.clean_input(input_str) == ""

    def split_on_n(self, inpt) -> list:
        if isinstance(inpt, list):
            return flatten([tkn.split("\n") for tkn in inpt])
        elif isinstance(inpt, str):
            return inpt.split("\n")
        else:
            print(inpt)
            print(type(inpt))
            raise ValueError("Please provide either string or list of strings.")

    def split_into_clean_sentences(self,input_str) -> list:
        sent_list = self.tknz_sent(input_str)
        sent_list = [self.clean_input(tkn) for tkn in sent_list]
        return sent_list


    def return_substr_location(self,sub_str,total_str):
        i1 = total_str.find(sub_str)
        if i1 >= 0:
            return (i1, i1+len(sub_str))
        else:
            return(i1,i1)

    def guess_language(self, inpt) -> tuple:
        # if more than one line, concat to one line
        if "\n" in inpt:
            inpt = "".join(inpt.split("\n"))
        label,est = self.ft_model.predict(inpt)
        lang = label[0].split("__")[2]
        prec_f = est[0]
        return lang, prec_f



class DataRetriever:

    def __init__(self) -> None:
        self.gtp = GeneralTextProcessor()

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
                    # todo MA: locate element via child.find_all_previous()
                    lang, est = self.gtp.guess_language(sentence)
                    res_list.append({'text': sentence, "html-tag": child.name, "lang":lang, "est":est})
        return res_list

    def remove_empty_rows(self,input_df) -> pd.DataFrame:
        return input_df.loc[~input_df.text.apply(self.gtp.check_if_text_empty),:]

    def tidy_text_row(self,input_df) -> pd.DataFrame:
        red_df = self.remove_empty_rows(input_df)
        red_df.loc[:,"text"] = red_df.loc[:,"text"].apply(self.gtp.clean_input)
        return red_df
