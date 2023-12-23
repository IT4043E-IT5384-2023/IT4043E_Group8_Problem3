import os
from typing import List
PROJECT_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../configs/")

from dotenv import load_dotenv
load_dotenv(f"{PROJECT_CONFIG}/acc_list.env")

from utils.read_configs import get_keywords

# for multi account crawler
def divide_kw_per_acc(id: int) -> List:
    num_acc = int(os.getenv("SCR_TW_NUM_ACC"))
    keywords = get_keywords()
    n_kw_per_acc = len(keywords) // num_acc

    return keywords[id*n_kw_per_acc:(id+1)*n_kw_per_acc]
