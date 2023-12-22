# v2
## env
import os
import sys
PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")
sys.path.append(PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv()
TW_LOGIN_USERNAME = os.getenv('SCR_TW_USERNAME')
TW_LOGIN_PASSWORD = os.getenv('SCR_TW_PASSWORD')

## utils
import yaml
import json
import datetime

from typing import List, Union
from kafka import KafkaProducer
from tweety import Twitter

from utils.log import logger
logger = logger("crawler_kf")

def read_yaml(path):
    with open(path, "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("Read YAML config successfully")

    return config

def get_tw_session(usn, psw) -> Twitter:
    '''
    Get Twitter session as variable

    Parameters
    ----------
    usn : str
        Twitter username
    psw : str
        Twitter password

    Returns
    -------
    app : Twitter
        Twitter session
    '''
    # Login Twitter account
    app = Twitter("session")
    app.sign_in(username = usn, password = psw)
    logger.info("Twitter session is created successfully.")
    return app

def crawl_tweet_kol(
    app,
    keywords: Union[str, List[str]],
    min_faves: int = 100,
    min_retweets: int = 10,
    pages: int = 10,
    wait_time: int = 30,
    airflow_mode: bool = False,
    time_delta_hour: int = 24
) -> List:
    for keyword in keywords:
        print(f"Crawling for keyword {keyword}")
        
        res = []

        search_param = f"{keyword}"
        search_param += f" min_faves:{min_faves}"
        search_param += f" min_retweets:{min_retweets}"
        
        if airflow_mode:
            # include timer
            search_param += f" until:{datetime.now().strftime('%Y-%m-%d')}"
            search_param += f" since:{(datetime.now() - datetime.timedelta(hours=time_delta_hour)).strftime('%Y-%m-%d')}"

        all_tweets = app.search(search_param, pages = pages, wait_time = wait_time)
        for tweet in all_tweets:
            tweet_data = tweet.__dict__
            res.append(tweet_data)
    
        logger.info(f"{keyword}: crawled {len(list(all_tweets))} tweets")

    return res