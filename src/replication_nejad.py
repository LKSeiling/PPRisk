from cProfile import label
from pickle import FALSE
from transformers import BertModel, BertConfig, BertTokenizer
from fast_bert.data_cls import BertDataBunch
from fast_bert.learner_cls import BertLearner
from box import Box

import torch

from pathlib import Path

from fast_bert.metrics import accuracy
from sklearn.metrics import classification_report
import logging
import pandas as pd

args = Box({
    "seed": 42,
    "task_name": 'imdb_reviews_lm',
    "model_name": 'roberta-base',
    "model_type": 'roberta',
    "train_batch_size": 16,
    "learning_rate": 4e-5,
    "num_train_epochs": 20,
    "fp16": True,
    "fp16_opt_level": "O2",
    "warmup_steps": 1000,
    "logging_steps": 0,
    "max_seq_length": 512,
    "multi_gpu": True if torch.cuda.device_count() > 1 else False
})


DATA_PATH = Path("/home/lks/Documents/IM_MA/Codebase/data/sources/benchmark_nejad/polisis_benchmark/datasets/Majority/")
LABEL_DIR = Path("/home/lks/Documents/IM_MA/Codebase/data/sources/benchmark_nejad/polisis_benchmark/datasets/")
MODEL_PATH = Path("/home/lks/Documents/IM_MA/Codebase/models/bert/finetuned_pripol_out")
LOG_PATH  = Path("/home/lks/Documents/IM_MA/Codebase/data/sources/benchmark_nejad/polisis_benchmark/logs")
OUT_PATH  = Path("/home/lks/Documents/IM_MA/Codebase/data/sources/benchmark_nejad/polisis_benchmark/output")


databunch_pripolis = BertDataBunch(data_dir=DATA_PATH, label_dir=LABEL_DIR,
                          tokenizer='bert-base-uncased',
                          train_file="train_dataset.csv",
                          test_data="test_dataset.csv",
                          val_file="validation_dataset.csv",
                          label_col=["Data Retention","Data Security","Do Not Track","First Party Collection/Use","International and Specific Audiences","Introductory/Generic","Policy Change","Practice not covered","Privacy contact information","Third Party Sharing/Collection","User Access, Edit and Deletion","User Choice/Control"],
                          batch_size_per_gpu=16,
                          max_seq_length=512,
                          multi_gpu=False,
                          multi_label=True,
                          model_type='bert')


logger = logging.getLogger()
device_cuda = torch.device("cuda")
metrics = [{'name': 'accuracy', 'function': accuracy}]

learner = BertLearner.from_pretrained_model(
						databunch_pripolis,
						pretrained_path=MODEL_PATH,
						metrics=metrics,
						device="cuda",
						logger=logger,
						output_dir=OUT_PATH,
						finetuned_wgts_path=None,
						warmup_steps=500,
						multi_gpu=True,
						is_fp16=True,
						multi_label=False,
						logging_steps=50)

t_df = pd.read_csv("/home/lks/Documents/IM_MA/Codebase/data/sources/benchmark_nejad/polisis_benchmark/datasets/Majority/test_dataset.csv")
text_list = t_df['text'].to_list()
preds = learner.predict_batch(text_list)
print(preds)
# classification_report(
#     y_expected,
#     y_pred,
#     output_dict=False,
#     target_names=['class A', 'class B', 'class C']
# )








