## consts
import os
import sys
PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")
sys.path.append(PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv()
load_dotenv(f"{PROJECT_ROOT}/configs/infra.env")
KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
KAFKA_TOPIC = os.getenv('CRL_KAFKA_TOPIC')

## env
import json
import argparse

from utils.custlog import custlogger
logger = custlogger("producer")
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from crawler.crawler_kf import get_tw_session, crawl_tweet_kol
from utils.read_configs import get_keywords, get_search_params, get_acc_by_index
from utils.divide_kw_per_acc import divide_kw_per_acc


def on_send_success(r):
    logger.info((r.topic, r.partition, r.offset))

def on_send_error(e):
    logger.error('message send error:', exc_info=e)

class Producer():
    def __init__(self, id: int, afmode: bool = False):
        # multi account index
        self.id = id
        self.afmode = afmode

        # create kafka topic
        self.create_topic(
            KAFKA_TOPIC,
            partition=3,
            replication_factor=1
        )

        # kafka producer
        self.producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
    def create_topic(self, topic_name: str, partition: int = 3, replication_factor: int = 1):
        try:
            admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BOOTSTRAP_SERVER)

            admin_client.create_topics(
                new_topics=[NewTopic(
                    name=topic_name,
                    num_partitions=partition,
                    replication_factor=replication_factor)],
                validate_only=False)

            logger.info(f"Trying to create topic: {topic_name}")
        except TopicAlreadyExistsError as e:
            logger.error(e)
            pass
    
    def produce(self):
        app = get_tw_session(*get_acc_by_index(self.id))
        search_params = get_search_params()
        crawled_data = crawl_tweet_kol(
            app=app,
            keywords=divide_kw_per_acc(self.id),
            min_faves=search_params['min_faves'],
            min_retweets=search_params['min_retweet'],
            pages=search_params['pages'],
            wait_time=search_params['wait_time'],
            airflow_mode=self.afmode,
            time_delta_hour=search_params['time_delta_hour']
        )

        for tweet in crawled_data:
            self.producer \
                .send(KAFKA_TOPIC, value=tweet) \
                .add_callback(on_send_success) \
                .add_errback(on_send_error)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, help="Producer ID", required=True)
    parser.add_argument("--airflow", action="store_true", help="Airflow mode", required=False, default=False)
    args = parser.parse_args()

    logger = custlogger("Producer " + str(args.id))
    producer = Producer(id=args.id, afmode=args.airflow)
    producer.produce()