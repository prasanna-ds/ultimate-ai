import re
from typing import List

import requests

from bs4 import BeautifulSoup
from pyspark.sql.functions import udf, col, lit
from pyspark.sql.types import IntegerType, StringType, ArrayType, Row
from requests import Response
from stream_processor.common import config


@udf(returnType=IntegerType())  # type: ignore
def get_total_case_count_udf() -> int:
    url: str = config.WORLDOMETER_URL
    req_data: Response = requests.get(url)
    soup: BeautifulSoup = BeautifulSoup(req_data.text, "html.parser")
    case_count: str = soup.find("title").text
    total_case_count: str = re.search(r"[0-9,]+", case_count).group(0)
    return int(total_case_count.replace(",", ""))


@udf(returnType=StringType())  # type: ignore
def process_tweet_udf(tweet: str) -> str:
    url_replaced_tweet: str = re.sub(r"http\S+", "", tweet)
    processed_tweet: str = re.sub(r"[#RT:]+", "", url_replaced_tweet)
    return processed_tweet.strip()

