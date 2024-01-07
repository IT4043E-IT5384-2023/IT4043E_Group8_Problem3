# System for labeling and evaluating the quality of accounts on Twitter

## Documentation

All documents are in [docs/](https://github.com/IT4043E-IT5384-2023/IT4043E_Group8_Problem3/tree/master/docs)

## Pre-requisites
* Linux OS (preferably with `bash`)
* Python >= 3.8 (Tested on Python 3.10.12)
* [Docker](docker.com) (Optional, local testing only)
* Twitter account
* JARS executables of the following:
  * [Spark-Elasticsearch connector](https://repo1.maven.org/maven2/org/elasticsearch/elasticsearch-spark-30_2.12/8.11.2/)
  * [Spark-GCS for Hadoop 3](https://cloud.google.com/dataproc/docs/concepts/connectors/cloud-storage)

## Instruction

0. Clone repo
```bash
git clone --recursive https://github.com/IT4043E-IT5384-2023/IT4043E_Group8_Problem3
cd IT4043E_Group8_Problem3/
```

1. (Optional) Create virtual Python environment
```bash
python -m venv bdt
source ./bdt/bin/activate 
```

2. Install Python requirements
```bash
pip install -r requirement.txt
```

3. In `configs/` directory, copy all `*.env.example` files to  `*.env` and fill in values accordingly.

4. Copy `elasticsearch-spark-30_2.12-8.11.2.jar` and `gcs-connector-hadoop3-latest.jar` to `spark/jars`. Then, mark them as executable
```bash
chmod +x -R ./spark/jars/
```

5. For local test, run Kafka/Zookeeper Docker image (provided by Conduktor)
```bash
docker compose -f ./kafka-stack-docker-compose/zk-single-kafka-single.yml up
```

6. Change keywords and other infos in `config_kw.yml` and `config_kol.yml`

7. Crawl manually (create 2 terminals):
```bash
python kafka/producer.py
python kafka/consumer.py
```

7.1. Crawl with Airflow (create 2 terminals):
```bash
airflow webserver
airflow scheduler
```

<details>
  <summary>Archive: Old version (non-Kafka)</summary>

0. Clone repo
```bash
git clone https://github.com/IT4043E-IT5384-2023/IT4043E_Group8_Problem3
cd IT4043E_Group8_Problem3/
```

2. (Optional) Create virtual Python environment
```bash
python -m venv tw_crawler
source ./tw_crawler/bin/activate 
```

2. Install Python requirements
```bash
pip install -r requirement.txt
```

3. Create a Twitter/X account and [sign up for API](https://developer.twitter.com)

4. Create a acc.txt file containing these information (only the values):
```
USERNAME
USER PASSWORD
BEARER KEY
```

5. Crawl away. Change keywords in config.yaml:
```bash
cd crawler/
mkdir -p data/
python crawler.py
```
</details>

## Dashboard
![img](https://github.com/IT4043E-IT5384-2023/IT4043E_Group8_Problem3/blob/8c63cd9000ce4ea3b7e7c210bcc872ecb4073398/docs/img/kibana-dashboard.png)

## Contact

Please report bugs to [Issues](https://github.com/IT4043E-IT5384-2023/IT4043E_Group8_Problem3/issues) or email to our members.
