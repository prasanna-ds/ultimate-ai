import re

import requests

from bs4 import BeautifulSoup
from pyspark.sql.functions import udf

from kafka_utilis.common import config


@udf
def get_total_case_count() -> int:
    url = config.WORLDOMETER_URL
    req_data = requests.get(url)
    soup = BeautifulSoup(req_data.text, "html.parser")
    case_count = soup.find("title").text
    total_case_count = re.search(r"[0-9,]+", case_count).group(0)
    return int(total_case_count.replace(",", ""))


@udf
def process_tweet(tweet: str) -> str:
    url_replaced_tweet = re.sub(r"http\S+", "", tweet)
    processed_tweet = re.sub(r"[#RT:]+", "", url_replaced_tweet.strip())
    return processed_tweet
