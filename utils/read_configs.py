import os
import sys
PROJECT_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../configs/")
sys.path.append(PROJECT_CONFIG)

import yaml
from utils.custlog import custlogger
logger = custlogger("read_configs")
from dotenv import load_dotenv
load_dotenv(f"{PROJECT_CONFIG}/acc_list.env")

def get_keywords():
    with open(f"{PROJECT_CONFIG}/config_kw.yaml", "r") as f:
        cf = yaml.load(f, Loader=yaml.FullLoader)
        logger.info("Read keywords successfully.")

    return cf['keywords']

def get_search_params():
    with open(f"{PROJECT_CONFIG}/config_kol.yaml", "r") as f:
        cf = yaml.load(f, Loader=yaml.FullLoader)
        logger.info("Read search params successfully.")

    return cf

def get_acc_by_index(index):
    usn = os.getenv(f"SCR_TW_USERNAME_{index}")
    psw = os.getenv(f"SCR_TW_PASSWORD_{index}")
    
    return (usn, psw)

    