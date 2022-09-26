from classes_general import DataRetriever
from utils import read_file, flatten

import pandas as pd
from tqdm import tqdm
from json import loads, dumps
from os import getcwd, listdir

pd.options.mode.chained_assignment = None  # default='warn'

class OPP_Retriever(DataRetriever):

    def __init__(self, threshold, level) -> None:
        super().__init__()

        self.set_threshold(threshold)
        self.set_level(level)

    def set_threshold(self, threshold):
        if threshold not in [0.5,0.75,1]:
            raise ValueError('No data exists for the supplied threshold. Please choose either 0.5,0.75, or 1.0 as threshold values.')
        self.threshold = threshold

    def set_level(self, level):
        if level not in ["sentences", "segments"]:
            raise ValueError('Invalid level value. Please provide either "sentences" or "segments" as values for the output level.')
        self.level = level

    def get_path_str(self, location, policy_id=None) -> str:
        if location == "sanitized" or location == "saved" or location == "cleaned":
            path_to_policies = "".join([getcwd(), "/data/sources/OPP-115/sanitized_policies/"])
        elif location == "consolidation":
            path_to_policies = "".join([getcwd(), "/data/sources/OPP-115/consolidation/threshold-",str(self.threshold),"-overlap-similarity/"])
        else:
            raise ValueError('Unknown location. Please choose either "sanitized","consolidation", "saved", or "cleaned".')
        
        policy_file_name = ""
        if policy_id:
            policy_file_name = [elem for elem in listdir(path_to_policies) if str(policy_id) == elem.split("_")[0]][0]

        if location == "saved" and policy_id:
            return "".join([getcwd(), "/data/saved/opp/labelled_",self.level,"/",policy_file_name,".csv"])
        elif location == "saved" and not policy_id:
            raise ValueError('Cannot save policy without policy_id.')
        elif location == "cleaned":
            return "".join([getcwd(), "/data/training_data/opp/",self.level,"/",policy_file_name,".csv"])
        else:
            return "".join([path_to_policies,policy_file_name])

    def get_policy_ids(self):
        ids_labels = [int(elem.split("_")[0]) for elem in listdir("".join([getcwd(), "/data/sources/OPP-115/consolidation/threshold-",str(self.threshold),"-overlap-similarity/"]))]
        ids_tetxs = [int(elem.split("_")[0]) for elem in listdir("".join([getcwd(), "/data/sources/OPP-115/sanitized_policies/"]))]
        no_match =[id for id in ids_tetxs if id not in ids_labels]
        if len(no_match) > 0:
            print("Fehlene Texte oder Label für id(s): {}".format(no_match))
        return [id for id in ids_tetxs if id in ids_labels]

    def return_cons_df(self, policy_id) -> pd.DataFrame:
        cons_df = pd.read_csv(self.get_path_str("consolidation",policy_id), header=None)
        try:
            cons_df = cons_df[cons_df[0].str.contains('C')]
            
        except AttributeError:
            cons_df = pd.DataFrame()
        return cons_df

    def return_segments(self,policy_id) -> list:
        policy_text = read_file(self.get_path_str("sanitized",policy_id))
        segments = policy_text.split("|||")
        return segments


#### SENTENCE LEVEL
    def return_sentence_level_data(self, policy_id=None):
        if not isinstance(policy_id, type(None)) & (type(policy_id) != int):
            raise ValueError('Supplied non-integer as policy_id. Please provide an integer value.')
        elif not isinstance(policy_id, type(None)):
            self.map_to_sentence_level(policy_id)
        else:
            san_path = self.get_path_str("sanitized")
            cons_path = self.get_path_str("consolidation")
            print("\nExtracting sentences from '{}' and mapping to consolidated labels from '{}'.".format(san_path,cons_path))
            policy_ids = self.get_policy_ids()
            for policy_id in tqdm(policy_ids, total=len(policy_ids)):
                self.map_to_sentence_level(policy_id)

    def map_to_sentence_level(self,policy_id):
        sent_df = self.return_sentence_info(policy_id)
        span_df = self.return_span_info(policy_id)

        if len(span_df) > 0:
            label_df = self.label_sentences(sent_df, span_df)
            label_df.insert(0, "policy_id", policy_id)
            #label_df = self.remove_empty_rows(label_df)
            sentences_path = self.get_path_str("saved",policy_id)
            label_df.to_csv(sentences_path, index=False, sep="\t")


    def return_sentence_info(self, policy_id) -> pd.DataFrame:
        segments = self.return_segments(policy_id)

        res_df = pd.DataFrame(columns=["segment", "idx_start", "idx_end", "text", "lang", "est"])
        for idx, segment in enumerate(segments):
            sentences = self.gtp.tknz_sent(segment)
            sentences = self.gtp.split_on_n(sentences)

            for sentence in sentences:
                start_idx, end_idx = self.gtp.return_substr_location(sentence,segment)
                lang, est = self.gtp.guess_language(sentence)
                new_row = {'segment':idx, 'idx_start':start_idx, 'idx_end': end_idx,
                            'text':sentence,'label':"untagged",'lang':lang, 'est':est}
                new_df = pd.DataFrame([new_row])
                res_df = pd.concat([res_df, new_df])

        return res_df   


    def return_span_info(self, policy_id):
        cons_df = self.return_cons_df(policy_id)

        if len(cons_df) == 0:
            return cons_df
        else:
            def return_labeled_spans(df_row):
                sub_label_json = loads(df_row[6])
                temp_span_df = pd.DataFrame(sub_label_json).transpose()
                temp_span_df = temp_span_df[temp_span_df['endIndexInSegment'] !=-1]
                temp_span_df['subtag'] = temp_span_df.index
                temp_span_df.reset_index(drop=True)
                return temp_span_df

            span_df = pd.DataFrame(columns=["endIndexInSegment", "startIndexInSegment", "value", "selectedText", "subtag"])
            for idx, row in cons_df.iterrows():
                row_spans = return_labeled_spans(row)
                row_spans.insert(0, "segment", int(row[4]))
                row_spans.insert(5, "supertag", row[5])
                span_df = pd.concat([span_df,row_spans])
            return span_df

    
    def label_sentences(self, sentence_df, span_df):

        def return_sentence_labels(sentence_row):   
            rel_spans = span_df[span_df['segment'] == sentence_row['segment']]
            
            behind_sen = sentence_row['idx_end'] < rel_spans['startIndexInSegment']
            before_sen = rel_spans['endIndexInSegment'] < sentence_row['idx_start']
            rel_spans = rel_spans[~ (behind_sen | before_sen)]

            res_dict = {}
            for idx, row in rel_spans.iterrows():
                key = row['supertag']
                # make row json function
                row_dict = row[["endIndexInSegment", "startIndexInSegment", "value", "selectedText", "subtag"]].to_dict()
                subtag_info = pd.DataFrame([row_dict])
                subtag_info = subtag_info.set_index("subtag")
                temp_dict = subtag_info.transpose().to_dict()
                try:
                    res_dict[key][idx] = dumps(temp_dict)
                except KeyError:
                    res_dict[key] = {idx:dumps(temp_dict)}
                    
                
            if len(res_dict) == 0:
                res_dict['Unlabelled'] = {}
            return res_dict

        sentence_df['label'] = sentence_df.apply(return_sentence_labels, axis=1)
        return sentence_df

    def clean_sentence_level_data(self):
        print("Removing html-only lines and lowercasing each sentence. Saving to '/data/training_data/opp/sentences/'.")
        labelled_path = "/data/saved/opp/labelled_sentences/"
        all_unclean = ["".join([getcwd(), labelled_path, path_unclean]) for path_unclean in listdir("".join([getcwd(), labelled_path]))]
        for path_unclean in tqdm(all_unclean, total=len(all_unclean)):
            policy_id = path_unclean.split("_")[-2].split("/")[-1]
            unclean_df = pd.read_csv(path_unclean, sep="\t")
            clean_df = self.tidy_text_row(unclean_df)
            clean_path = self.get_path_str("cleaned",policy_id)
            clean_df.to_csv(clean_path, index=False, sep="\t")


### SEGEMNT LEVEL
    def return_segment_level_data(self, policy_id, method=""):
        pass