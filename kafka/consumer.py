import os
import sys
PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../")
sys.path.append(PROJECT_ROOT)

from dotenv import load_dotenv
load_dotenv()
load_dotenv(f"{PROJECT_ROOT}/configs/infra.env")
KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')
KAFKA_TOPIC = os.getenv('CRL_KAFKA_TOPIC')

import json
import pyspark as spark

from kafka import KafkaConsumer
from utils.log import logger
logger = logger("Consumer 0")

class Consumer():
    def __init__(self):
        # kafka producer
        self.consumer = KafkaConsumer(
            group_id="consumer0",
            auto_offset_reset='earliest',
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=2000
        )

        self.consumer.subscribe([KAFKA_TOPIC])

    def consume(self):
        # stream kafka message to spark directly, no separate consumer
        pass

if __name__ == "__main__":
    consumer = Consumer()
    consumer.consume()