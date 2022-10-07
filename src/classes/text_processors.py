from cgitb import text
import re
from time import sleep
import requests
import fasttext
import pandas as pd
import html2text


from os import getcwd
from html import unescape
from utils import flatten
from html import unescape
from unidecode import unidecode
from markdown import markdown as md2html

from nltk import tokenize
from bs4 import BeautifulSoup
from difflib import SequenceMatcher

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
            inpt = inpt.replace("\n", "")
        label,est = self.ft_model.predict(inpt)
        lang = label[0].split("__")[2]
        prec_f = est[0]
        return lang, prec_f
    
    def clean_url(self, url, prefix=None):
        no_prefix = re.sub(r'(http.{0,1}:\/\/){0,1}(www.){0,1}(.*)',r'\3', url)  
        if no_prefix[-1] == "#":
            no_prefix = no_prefix[0:-1]
        if no_prefix[-1] == "/":
            no_prefix = no_prefix[0:-1]

        if prefix:
            return "".join([prefix,no_prefix])
        else:
            return no_prefix

    def similar(self, a, b):
        return SequenceMatcher(None, a, b).ratio()


class HTML_Cleaner(GeneralTextProcessor):

    def __init__(self, CLEANING_PIPELINE = None) -> None:
        super().__init__()
        if not CLEANING_PIPELINE:
            self.CLEANING_PIPELINE = [{"obj_name":"header"}, 
            {"obj_name":"div", "attr_dict":{"class*":"header"}}, 
            {"obj_name":"div", "attr_dict":{"id*":"header"}},
            
            {"obj_name":"footer"},
            {"attr_dict":{"class*":"footer"}},
            {"attr_dict":{"id*":"footer"}},
            
            {"obj_name":"nav"},
            {"attr_dict":{"class*":"nav"}},
            {"attr_dict":{"id*":"nav"}},
            {"attr_dict":{"role":"navigation"}},
            
            {"attr_dict":{"class*":"sidebar"}},
            {"attr_dict":{"id*":"sidebar"}},
            
            {"obj_name":"img"},
            {"obj_name":"svg"},
            {"obj_name":"button"},
            {"attr_dict":{"class*":"btn"}},
            
            {"attr_dict":{"class*":"menu"}},
            
            {"obj_name":"select"},
            {"obj_name":"noscript"},
            {"obj_name":"aside"},
            {"obj_name":"ul", "attr_dict":{'role':'navigation'}},
            {"obj_name":"div", "attr_dict":{'role':'list'}},
            {"obj_name":"li", "attr_dict":{"class*":"flags"}},
            {"obj_name":"div", "attr_dict":{"class*":"favs"}},
            {"obj_name":"span", "attr_dict":{"class*":"screen"}},
            
            {"attr_dict":{"class*":"toolbar"}},
            {"attr_dict":{"class*":"menu"}},
            {"attr_dict":{"id*":"main-navigation"}},
            {"obj_name":"div", "attr_dict":{"class*":"hidden"}},
            {"obj_name":"div", "attr_dict":{"class*":"lang"}},
            {"attr_dict":{"class*":"window"}},
            {"attr_dict":{"class*":"flags"}},
            {"attr_dict":{"class*":"Cookie"}},
            {"attr_dict":{"class*":"cookie"}},
            {"attr_dict":{"id*":"cookie"}},
            {"attr_dict":{"class*":"modal"}},
            {"attr_dict":{"class*":"Bar"}},
            {"attr_dict":{"class*":"divider"}},
            {"attr_dict":{"class*":"promo"}},
            {"attr_dict":{"class*":"screen-reader"}},
            {"attr_dict":{"class*":"expired"}},
            {"attr_dict":{"class*":"signup"}},
            {"attr_dict":{"class*":"search"}},
            {"attr_dict":{"class*":"additional"}},
            {"attr_dict":{"id*":"consent"}},
            {"attr_dict":{"id*":"copyright"}},
            {"attr_dict":{"id*":"skip"}},
            {"attr_dict":{"class*":"skip"}}]
        else:
            self.CLEANING_PIPELINE = CLEANING_PIPELINE

    def create_detect_str(self, obj_name="*", attr_dict=None):
        xpath_detect = '{}'.format(obj_name)
        for key, value in attr_dict.items():
            attr_str = '[{}="{}"]'.format(key, value)
            xpath_detect = ''.join([xpath_detect,attr_str])
        return xpath_detect

    def remove_html_element(self, html_str, obj_name="*", attr_dict=None):
        soup = BeautifulSoup(html_str)
        if attr_dict:
            xml_detect = self.create_detect_str(obj_name, attr_dict)
            for elem in soup.select(xml_detect):
                if elem.name != 'body':
                    elem.decompose()
        else:
            for elem in soup.find_all(obj_name): 
                if elem.name != 'body':
                    elem.decompose()
        return unescape(str(soup))

    def try_to_remove_html_element(self, input_html, obj_name="*", attr_dict=None):
        len_before = len(self.html_to_md(input_html))
        output_html = self.remove_html_element(input_html, obj_name=obj_name, attr_dict=attr_dict)
        len_after = len(self.raw_html_to_md(output_html))
        if len_after/len_before < 0.3:
            return input_html
        else:
            return output_html 
                                        
    def clean_html(self, html_str):
        
        html_out = html_str[:]
        
        for operation in self.CLEANING_PIPELINE:
            if "obj_name" in operation.keys() and "attr_dict" in operation.keys():
                html_out = self.try_to_remove_html_element(html_out, obj_name=operation["obj_name"], attr_dict=operation["attr_dict"])
            elif "obj_name" in operation.keys():
                html_out = self.try_to_remove_html_element(html_out, obj_name=operation["obj_name"])
            elif "attr_dict" in operation.keys():
                html_out = self.try_to_remove_html_element(html_out, attr_dict=operation["attr_dict"])
            else:
                pass
        
        return html_out

    def get_segment_type(self, input_str):
        heading_pattern = re.compile(r".*# .*|^[*]{2}.*[*]{2}$|^[A-Z][.] .*$|^\d*\\*[.] .*$")
        heading_match = heading_pattern.match(input_str)
        if heading_match and "<li>" not in md2html(input_str):
            return "heading"
        elif "<li>" in md2html(input_str):
            return "list"
        else:
            return "paragraph"

    def strip_and_remove_empty(self, input_list):
        clean_list = [elem.strip() for elem in input_list]
        clean_list = [elem for elem in clean_list if elem != ""]
        return clean_list

    def replace_in_sentence_linebreak(self, input_string):
        return re.sub(r'(.)\n([^ \\])',r'\1 \2', input_string)         
            
    def remove_html_tags(self, html_str, but=["br"]):
        html_soup = BeautifulSoup(html_str, features="html.parser")
        for tag in html_soup.find_all(True):
            if tag.name not in but:
                tag.replaceWithChildren()
        res_html = str(html_soup)
        res_html = res_html.strip()
        res_html = res_html.replace("<br/>", "<br>")
        return res_html

    def merge_lists(self, list_html1, list_html2):
        list_pattern = re.compile(r"<.l>")
        if list_pattern.match(list_html1).group() == list_pattern.match(list_html2).group():
            list_html1 = re.sub(r'</.l>', '', list_html1)
            list_html2 = re.sub(r'<.l>', '', list_html2)
        return " ".join([list_html1,list_html2])
    
    def sanitize_md(self, md_text, segment_type, keep = ["ul","ol", "li", "br", "b", "strong"]):
        html_text = md2html(md_text)
        if segment_type == "heading":
            heading_text = self.replace_html_tags(html_text)
            sanitized_segment = "".join(["<strong>",heading_text,"</strong>"])
        else:
            sanitized_segment = self.remove_html_tags(html_text, but = keep)
            sanitized_segment = sanitized_segment.replace("<b>","<strong>")
            sanitized_segment = sanitized_segment.replace("</b>","</strong>")
        return sanitized_segment

    def raw_html_to_md(self, html, ignore_links = False, bypass_tables = False):
        text_maker = html2text.HTML2Text()
        text_maker.ignore_links = ignore_links
        text_maker.bypass_tables = bypass_tables
        text = text_maker.handle(html)
        text = unidecode(text)
        return text

    def merge_and_sanitize_segments(self, md_segments):
        res_segments = []
        temp_segment = ""
        prev_type = ""
        for idx, md_segment in enumerate(reversed(md_segments)):
            text = self.replace_in_sentence_linebreak(md_segment)
            segment_type = self.get_segment_type(text)
            sanitized_text = self.sanitize_md(text, segment_type) 
            
            if idx == 0:
                temp_segment = sanitized_text
            elif segment_type == "paragraph" and (prev_type == "heading" or prev_type == "paragraph"):
                res_segments.append(temp_segment)
                temp_segment = sanitized_text
            else:
                if prev_type == "list" and segment_type == "list":
                    temp_segment = self.merge_lists(sanitized_text, temp_segment)
                else:
                    temp_segment = " <br> <br> ".join([sanitized_text, temp_segment])
                    
            if idx == len(md_segments)-1:
                res_segments.append(temp_segment)
                
            prev_type = segment_type
                
        return reversed(res_segments)

    def md_to_sanitized_html(self, text):
        all_segments = text.split("\n\n")
        all_segments = self.strip_and_remove_empty(all_segments)
        merged_segments = self.merge_and_sanitize_segments(all_segments)
        merged_segments = ["".join([" <br> <br>||| ", segment]) if idx > 0 else segment for idx, segment in enumerate(merged_segments) ]
        return "".join(merged_segments)

    def html_to_sanitized(self, html_str):
        md_text = self.raw_html_to_md(html_str, ignore_links=True)
        sanitized_html = self.md_to_sanitized_html(md_text)
        return sanitized_html