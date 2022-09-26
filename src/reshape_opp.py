from operator import le
from dr_opp import OPP_Retriever

THRESHOLD = 0.75
LEVEL = "sentences"

dr_opp = OPP_Retriever(threshold=THRESHOLD, level=LEVEL)
dr_opp.return_sentence_level_data() # for each policy
dr_opp.clean_sentence_level_data()

    