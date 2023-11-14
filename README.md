# Twitter Data

## Pre-requisites
* Python >= 3.8 (Tested on Python 3.10.12)

## Instruction

1. (Optional) Create virtual Python environment
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
python crawler/crawler.py
```
