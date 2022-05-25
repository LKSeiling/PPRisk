from classes_general import DataRetriever
from utils import read_file

import pandas as pd
from os import getcwd, listdir

class OPP_Retriever(DataRetriever):

    def __init__(self) -> None:
        super().__init__()

    def get_path_str(self, policy_id, location, threshold=None):
        if location == "sanitized" or location == "saved":
            path_to_policies = "".join([getcwd(), "/data/sources/OPP-115/sanitized_policies/"])
        elif location == "consolidation":
            path_to_policies = "".join([getcwd(), "/data/sources/OPP-115/consolidation/threshold-",str(threshold),"-overlap-similarity/"])
        else:
            raise ValueError('Unknown location. Please choose either "sanitized","consolidation", or "saved.')
        
        policy_file_name = [elem for elem in listdir(path_to_policies) if policy_id == int(elem.split("_")[0])][0]

        if location == "saved":
            return "".join([getcwd(), "/data/saved/opp/labelled_policies/",policy_file_name])
        else:
            return "".join([path_to_policies,policy_file_name])


    def retrieve_complete_policy_info(self, policy_id, threshold=0.75):
        if threshold not in [0.5,0.75,1]:
            raise ValueError('No data exists for the supplied threshold. Please choose either 0.5,0.75, or 1.0 as threshold values.')

        policy_text = read_file(self.get_path_str(policy_id,"sanitized"))
        segments = policy_text.split("|||")
        sent_df = self.return_segment_info(segments)
        cons_df = self. return_cons_df(policy_id, threshold)

        if len(cons_df) > 0:
            label_df = self.return_label_info(sent_df, cons_df)
            label_df.to_csv(self.get_path_str(policy_id,"saved"), index=False, sep="\t")

    def return_segment_info(self, segments) -> pd.DataFrame:
        res_df = pd.DataFrame(columns=["segment", "idx_start", "idx_end", "text", "lang", "prec"])
        for idx, segment in enumerate(segments):
            sentences = self.gtp.tknz_sent(segment)
            for sentence in sentences:
                start_idx, end_idx = self.gtp.return_substr_location(sentence,segment)
                lang, prec = self.gtp.guess_language(sentence)
                new_row = {'segment':idx, 'idx_start':start_idx, 'idx_end': end_idx,
                            'text':sentence,'label':"untagged",'lang':lang, 'prec':prec}
                res_df = res_df.append(new_row,ignore_index=True)
        return res_df

    def return_cons_df(self, policy_id, threshold):
        cons_df = pd.read_csv(self.get_path_str(policy_id,"consolidation",threshold), header=None)
        cons_df = cons_df[cons_df[0].str.contains('C')]
        return cons_df

    
    def return_label_info(self, sentence_df, consolidated_df):
        pass
