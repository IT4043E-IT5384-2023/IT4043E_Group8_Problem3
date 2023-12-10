# Twitter Data

## Pre-requisites
* Python >= 3.8 (Tested on Python 3.10.12)
* [Docker](docker.com)

## Instruction
<details>
  <summary>Old version (non-Kafka)</summary>

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

<details>
  <summary>Kafka-based (Not working // 401 Forbidden)</summary>

0. Clone repo
```bash
git clone --recursive https://github.com/IT4043E-IT5384-2023/IT4043E_Group8_Problem3
cd IT4043E_Group8_Problem3/
cd kafka/
```

1. (Optional) Create virtual Python environment
```bash
python -m venv tw_crl_kafka
source ./tw_crl_kafka/bin/activate 
```

2. Install Python requirements
```bash
pip install -r requirement.txt
```

3. Create a Twitter/X account and [sign up for API](https://developer.twitter.com)

4. Copy `.env.example` to `.env` and fill in accordingly (from the dev account)

5. Change keywords and other infos in `config_kw.yml` and `config_kol.yml`

6. Run Kafka/Zookeeper Docker image (provided by Conduktor)
```bash
docker compose -f ./kafka-stack-docker-compose/zk-single-kafka-single.yml up
```
z
7. Crawl away:
```bash
python tw_streaming.py
```
</details>
