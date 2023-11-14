import pandas as pd
import os
import json
import sys
import yaml

from typing import List, Union
from tweety import Twitter

def read_yaml(path):
    with open(path, "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("Read YAML config successfully")

    return config

current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

# Specify the JSON filename
json_filename = 'output.json'

def convert_to_json(data, json_filename=json_filename):
    # Open the JSON file in write mode
    data = [i for n, i in enumerate(data) if i not in data[:n]]
    with open(os.path.join("data", json_filename), 'w', encoding='utf-8') as json_file:
        # Write the data to the JSON file
        json_file.write('[')
        for idx, tweet in enumerate(data):
            json.dump(tweet, json_file, ensure_ascii=False, indent=4, default=str)
            if idx < len(data) - 1:
                json_file.write(',')  # Add a comma between objects
            json_file.write('\n')
        json_file.write(']')

def crawl_tweet_kol(
    app,
    keywords: Union[str, List[str]],
    min_faves: int = 100,
    min_retweets: int = 10,
    pages: int = 10,
    wait_time: int = 30
) -> List[pd.DataFrame]:
    for keyword in keywords:
        print(f"Crawling with keyword '{keyword}'")

        all_tweets = app.search(f"{keyword} min_faves:{min_faves} min_retweets:{min_retweets}", pages = pages, wait_time = wait_time)
        convert_to_json(all_tweets,f"{keyword}.json")
        for tweet in all_tweets:
            print(tweet.__dict__)
            return tweet

if __name__ == "__main__":
    # Read config file
    CONFIG_PATH = os.path.join(os. getcwd(), "config.yaml")
    config = read_yaml(path=CONFIG_PATH)

    # Login Twitter account
    app = Twitter("session")
    with open("acc.txt", "r") as f:
        username, password, key = f.read().split()
    app.sign_in(username, password, extra=key)

    tweet = crawl_tweet_kol(
        app = app,
        keywords=config['keywords'],
        min_faves=config['min_faves'],
        min_retweets=config['min_retweet'],
        pages=config['pages'],
        wait_time=config['wait_time']
    )