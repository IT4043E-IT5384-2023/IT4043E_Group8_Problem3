from typing import List, Union
from tweety import Twitter
import pandas as pd
import argparse
import yaml
import os
import json
import sys
current = os.path.dirname(os.path.realpath(__file__))
parent = os.path.dirname(current)
sys.path.append(parent)

def read_yaml(path):
    with open(path, "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("Read YAML config successfully")

    return config

# Specify the JSON filename
json_filename = 'output.json'
def convert_to_json(data, json_filename=json_filename):
    # Open the JSON file in write mode
    data = [i for n, i in enumerate(data) if i not in data[:n]]
    with open(os.path.join("data", json_filename), 'w', encoding='utf-8') as json_file:
        # Write the data to the JSON file
        for tweet in data:
          json.dump(tweet, json_file, ensure_ascii=False, indent=4, default = str)

def crawl_tweet_kol(
    app,
    keywords: Union[str, List[str]],
    min_faves: int = 100,
    min_retweets: int = 10,
    pages: int = 10,
    wait_time: int = 30
) -> List[pd.DataFrame]:
    
    """
    Crawl tweets and KOL accounts from Twitter.

    Args:
        app (TwitterApp): The Twitter app instance used for authentication.
        keywords (Union[str, List[str]]): Keywords used to search for tweets.
            Can be a single string or a list of strings.
        min_faves (int): Minimum number of likes for a tweet to be included.
        min_retweets (int): Minimum number of retweets for a tweet to be included.
        pages (int): Number of scroll down refreshing times during the crawling.
        wait_time (int): Interval to wait between 2 pages in seconds.

    Returns:
        List[pd.DataFrame, pd.DataFrame]: A tuple containing two pandas DataFrames.
            The first DataFrame is for tweets data, and the second is for KOLs data.

    """
    
    # 1st table columns
    keyword_used = []
    tweet_id = []
    tweet_body = []
    date = []
    is_sensitive = []
    like_counts = []
    reply_counts = []
    quote_counts = []
    bookmark_counts = []
    hashtags = []
    author_id = []
    views = []
    retweet_counts = []
    url = []

    # 2nd table columns
    user_id = []
    username = []
    name = []
    created_at = []
    is_verified = []
    media_count = []
    statuses_count = []
    favourites_count = []
    followers_count = []
    profile_url = []
    possibly_sensitive = []
    screen_name = []
    listed_count = []
    normal_followers_count = []
    fast_followers_count = []
    friends_count = []

    for keyword in keywords:
        print(f"Crawling for keyword {keyword}")
        
        all_tweets = app.search(f"{keyword} min_faves:{min_faves} min_retweets:{min_retweets}", pages = pages, wait_time = wait_time)
        convert_to_json(all_tweets,f"{keyword}.json")
        for tweet in all_tweets:
            tweet_data = tweet.__dict__
            author_data = tweet['author'].__dict__
            
            # Twitter // KoL
            keyword_used.append(keyword)
            tweet_id.append(tweet_data['id'])
            tweet_body.append(tweet_data['tweet_body'])
            date.append(tweet_data['date'])
            is_sensitive.append(tweet_data['is_sensitive'])
            like_counts.append(tweet_data['likes'])
            reply_counts.append(tweet_data['reply_counts'])
            quote_counts.append(tweet_data['quote_counts'])
            bookmark_counts.append(tweet_data['bookmark_count'])
            hashtags.append(tweet_data['hashtags'])
            author_id.append(author_data['id'])
            views.append(tweet_data['views'])
            retweet_counts.append(tweet_data['retweet_counts'])
            url.append(tweet_data['url'])
            
            # Twitter // raw
            user_id.append(author_data['id'])
            username.append(author_data['username'])
            name.append(author_data['name'])
            screen_name.append(author_data['screen_name'])
            created_at.append(author_data['created_at'])
            is_verified.append(author_data['verified'])
            media_count.append(author_data['media_count'])
            listed_count.append(author_data['listed_count'])
            statuses_count.append(author_data['statuses_count'])
            favourites_count.append(author_data['favourites_count'])
            followers_count.append(author_data['followers_count'])
            normal_followers_count.append(author_data['normal_followers_count'])
            fast_followers_count.append(author_data['fast_followers_count'])
            friends_count.append(author_data['friends_count'])
            possibly_sensitive.append(author_data['possibly_sensitive'])
            profile_url.append(author_data['profile_url'])
            
    first_table_df = pd.DataFrame({
        'keyword_used': keyword_used,
        'tweet_id': tweet_id,
        'tweet_body': tweet_body,
        'date': date,
        'is_sensitive': is_sensitive,
        'like_counts': like_counts,
        'reply_counts': reply_counts,
        'quote_counts': quote_counts,
        'bookmark_counts': bookmark_counts,
        'hashtags': hashtags,
        'author_id': author_id,
        'views': views,
        'retweet_counts': retweet_counts,
        'url': url

    })

    second_table_df = pd.DataFrame({
        'user_id': user_id,
        'username': username,
        'name': name,
        'screen_name' : screen_name,
        'created_at': created_at,
        'is_verified': is_verified,
        'listed_count': listed_count,
        'media_count': media_count,
        'statuses_count': statuses_count,
        'favourites_count': favourites_count,
        'followers_count': followers_count,
        'normal_followers_count': normal_followers_count,
        'friends_count': friends_count,
        'possibly_sensitive' : possibly_sensitive,
        'fast_followers_count' : fast_followers_count,
        'profile_url': profile_url
    })

    second_table_df.drop_duplicates(keep='first', inplace=True)
    second_table_df.reset_index(drop=True, inplace=True)
    
    return [first_table_df, second_table_df]

def crawl_tweet_kol_with_date(
    app,
    keywords: Union[str, List[str]],
    min_faves: int = 100,
    min_retweets: int = 10,
    pages: int = 10,
    wait_time: int = 30
) -> List[pd.DataFrame]:
    
    """
    Crawl tweets and KOL accounts from Twitter upto the last date in crawled data

    Args:
        app (TwitterApp): The Twitter app instance used for authentication.
        keywords (Union[str, List[str]]): Keywords used to search for tweets.
            Can be a single string or a list of strings.
        min_faves (int): Minimum number of likes for a tweet to be included.
        min_retweets (int): Minimum number of retweets for a tweet to be included.
        pages (int): Number of scroll down refreshing times during the crawling.
        wait_time (int): Interval to wait between 2 pages in seconds.

    Returns:
        List[pd.DataFrame, pd.DataFrame]: A tuple containing two pandas DataFrames.
            The first DataFrame is for tweets data, and the second is for KOLs data.

    """
    
    # Read the already crawled data
    tweets_table_df = pd.read_csv("data/tweets_table.csv", encoding='utf-8')
    kols_table_df = pd.read_csv("data/kols_table.csv", encoding='utf-8')
    
   # 1st table columns
    keyword_used = []
    tweet_id = []
    tweet_body = []
    date = []
    is_sensitive = []
    like_counts = []
    reply_counts = []
    quote_counts = []
    bookmark_counts = []
    hashtags = []
    author_id = []
    views = []
    retweet_counts = []
    url = []

    # 2nd table columns
    user_id = []
    username = []
    name = []
    created_at = []
    is_verified = []
    media_count = []
    statuses_count = []
    favourites_count = []
    followers_count = []
    profile_url = []
    possibly_sensitive = []
    screen_name = []
    listed_count = []
    normal_followers_count = []
    fast_followers_count = []
    friends_count = []

    for keyword in keywords:
        # Get the furthest date from the crawled data
        furthest_date = pd.to_datetime(tweets_table_df[tweets_table_df['keyword_used'] == keyword]['date']).sort_values().iloc[0]
        until_date = furthest_date - pd.Timedelta(days=1)
        until_date_str = str(until_date.date())
        
        print(f"Crawling for keyword {keyword} from {until_date_str.replace('-', '_')} backward")
        
        all_tweets = app.search(f"{keyword} min_faves:{min_faves} min_retweets:{min_retweets} until:{until_date_str}",
                                pages = pages, wait_time = wait_time)
        convert_to_json(all_tweets,f"{keyword}_upto_{until_date_str.replace('-', '_')}.json")
        for tweet in all_tweets:
            tweet_data = tweet.__dict__
            author_data = tweet['author'].__dict__
            
            # 1st table data
            keyword_used.append(keyword)
            tweet_id.append(tweet_data['id'])
            tweet_body.append(tweet_data['tweet_body'])
            date.append(tweet_data['date'])
            is_sensitive.append(tweet_data['is_sensitive'])
            like_counts.append(tweet_data['likes'])
            reply_counts.append(tweet_data['reply_counts'])
            quote_counts.append(tweet_data['quote_counts'])
            bookmark_counts.append(tweet_data['bookmark_count'])
            hashtags.append(tweet_data['hashtags'])
            author_id.append(author_data['id'])
            views.append(tweet_data['views'])
            retweet_counts.append(tweet_data['retweet_counts'])
            url.append(tweet_data['url'])
            
            # 2nd table data
            user_id.append(author_data['id'])
            username.append(author_data['username'])
            name.append(author_data['name'])
            screen_name.append(author_data['screen_name'])
            created_at.append(author_data['created_at'])
            is_verified.append(author_data['verified'])
            media_count.append(author_data['media_count'])
            listed_count.append(author_data['listed_count'])
            statuses_count.append(author_data['statuses_count'])
            favourites_count.append(author_data['favourites_count'])
            followers_count.append(author_data['followers_count'])
            normal_followers_count.append(author_data['normal_followers_count'])
            fast_followers_count.append(author_data['fast_followers_count'])
            friends_count.append(author_data['friends_count'])
            possibly_sensitive.append(author_data['possibly_sensitive'])
            profile_url.append(author_data['profile_url'])
            
    new_tweets_table_data = pd.DataFrame({
        'keyword_used': keyword_used,
        'tweet_id': tweet_id,
        'tweet_body': tweet_body,
        'date': date,
        'is_sensitive': is_sensitive,
        'like_counts': like_counts,
        'reply_counts': reply_counts,
        'quote_counts': quote_counts,
        'bookmark_counts': bookmark_counts,
        'hashtags': hashtags,
        'author_id': author_id,
        'views': views,
        'retweet_counts': retweet_counts,
        'url': url

    })


    new_kols_table_data = pd.DataFrame({
        'user_id': user_id,
        'username': username,
        'name': name,
        'screen_name' : screen_name,
        'created_at': created_at,
        'is_verified': is_verified,
        'listed_count': listed_count,
        'media_count': media_count,
        'statuses_count': statuses_count,
        'favourites_count': favourites_count,
        'followers_count': followers_count,
        'normal_followers_count': normal_followers_count,
        'friends_count': friends_count,
        'possibly_sensitive' : possibly_sensitive,
        'fast_followers_count' : fast_followers_count,
        'profile_url': profile_url
    })
    
    tweets_table_df = tweets_table_df.append(new_tweets_table_data, ignore_index=True)
    
    kols_table_df = kols_table_df.append(new_kols_table_data, ignore_index=True)
    kols_table_df.drop_duplicates(keep='first', inplace=True)
    kols_table_df.reset_index(drop=True, inplace=True)
    
    return [tweets_table_df, kols_table_df]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--crawl_from_last_date", action='store_true', help="whether to continue crawling from the last date")
    args = parser.parse_args()
    
    # Read config file
    CONFIG_PATH = os.path.join(os. getcwd(), "config.yaml")
    config = read_yaml(path=CONFIG_PATH)
    
    # Login Twitter account
    app = Twitter("session")
    with open("acc.txt", "r") as f:
        username, password, key = f.read().split()
    app.sign_in(username, password, extra=key)
    
    if args.crawl_from_last_date:
        # Crawl tweets and kols
        tweets_df, kols_df = crawl_tweet_kol_with_date(
            app = app,
            keywords=config['keywords'],
            min_faves=config['min_faves'],
            min_retweets=config['min_retweet'],
            pages=config['pages'],
            wait_time=config['wait_time']
        )
    else:
        # Crawl tweets and kols
        tweets_df, kols_df = crawl_tweet_kol(
            app = app,
            keywords=config['keywords'],
            min_faves=config['min_faves'],
            min_retweets=config['min_retweet'],
            pages=config['pages'],
            wait_time=config['wait_time']
        )
    
    # Save data
    tweets_df.to_csv("data/tweets_table.csv", index=False, encoding='utf-8')
    kols_df.to_csv("data/kols_table.csv", index=False, encoding='utf-8')