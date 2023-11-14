import os
import sys
import logging
import yaml
import time

from typing import List, Union
from json import dumps
from dotenv import load_dotenv
from kafka import KafkaProducer
from tweety import Twitter
load_dotenv()

logging.basicConfig(
    filename='app.log',
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)


def read_yaml(path):
    with open(path, "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("Read YAML config successfully")

    return config

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
TW_LOGIN_USERNAME = os.getenv('SCR_TW_USERNAME')
TW_LOGIN_PASSWORD = os.getenv('SCR_TW_PASSWORD')
TW_BEARER_KEY = os.getenv('TWITTER_BEARER_KEY')

########

def crawl_tweet_kol(
    app,
    kf_producer,
    keywords: Union[str, List[str]],
    min_faves: int = 100,
    min_retweets: int = 10,
    pages: int = 10,
    wait_time: int = 30
):
    while True:
        for keyword in keywords:
            print(f"Crawling with keyword '{keyword}'")
            logging.info(f"Crawling with keyword '{keyword}'")

            all_tweets = app.search(f"{keyword} min_faves:{min_faves} min_retweets:{min_retweets}", pages = pages, wait_time = wait_time)
            for tweet in all_tweets:
                kf_producer.send(keyword, value=tweet.__dict__)
                logging.info(f"Send tweet with id: {tweet.id} to Kafka successfully.")

        print("Sleeping for 30m...")
        logging.info("Sleeping for 30m...")
        time.sleep(1800)

# Tweety-ns init
app = Twitter("session")

app.sign_in(
    username = TW_LOGIN_USERNAME,
    password = TW_LOGIN_PASSWORD,
    extra = TW_BEARER_KEY
)

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[f'{KAFKA_BOOTSTRAP_SERVER}'],
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

logging.info('Kafka producer is initiated.')

# Read config files
KEYWORDS_CONFIG_PATH = os.path.join(os.getcwd(), "config_kw.yaml")
config_kw = read_yaml(path=KEYWORDS_CONFIG_PATH)
KOL_CONFIG_PATH = os.path.join(os.getcwd(), "config_kol.yaml")
config_kol = read_yaml(path=KOL_CONFIG_PATH)

logging.info('Read configs successfully.')

# Crawl tweets
tweet = crawl_tweet_kol(
    app = app,
    kf_producer = producer,
    keywords=config_kw['keywords'],
    min_faves=config_kol['min_faves'],
    min_retweets=config_kol['min_retweet'],
    pages=config_kol['pages'],
    wait_time=config_kol['wait_time']
)
