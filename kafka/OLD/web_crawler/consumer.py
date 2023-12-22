import os
import json
import yaml

from kafka import KafkaConsumer
from dotenv import load_dotenv
load_dotenv()

KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER')


def read_yaml(path):
    with open(path, "r") as yamlfile:
        config = yaml.load(yamlfile, Loader=yaml.FullLoader)
        print("Read YAML config successfully")

    return config

def convert_to_json(data, json_filename="default.json"):
    # Open the JSON file in write mode
    data = [i for n, i in enumerate(data) if i not in data[:n]]
    with open(os.path.join("data", json_filename), 'w', encoding='utf-8') as json_file:
        # Write the data to the JSON file
        for tweet in data:
          json.dump(tweet, json_file, ensure_ascii=False, indent=4, default = str)


# Read config files
KEYWORDS_CONFIG_PATH = os.path.join(os.getcwd(), "config_kw.yaml")
config_kw = read_yaml(path=KEYWORDS_CONFIG_PATH)

consumer = KafkaConsumer(
    'tw_crl',
    bootstrap_servers=[f'{KAFKA_BOOTSTRAP_SERVER}'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

for message in consumer:
    print(message)
    message = message.value
    convert_to_json(message, f"{message['keyword']}.json")
    print('-----------------------')

