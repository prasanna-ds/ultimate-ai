import logging
import re

from typing import Match, Optional

import requests

from bs4 import BeautifulSoup
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType
from requests import Response
from requests.exceptions import RequestException

from stream_processor.common import config
from stream_processor.common.utils import Cache


URL: str = config.WORLDOMETER_URL


@udf(returnType=IntegerType())  # type: ignore
def get_total_case_count_udf() -> int:
    cache = Cache()
    cache.set("total_case_count", -1)
    print("start udf....")
    try:
        req_data: Response = requests.get(URL, timeout=(5, 10))
        print("request successful....")
    except RequestException as e:
        logging.error(f"Exception occured while making a request to {URL}, e")
        print("request exception....")
        return cache.get("total_case_count")

    try:
        soup: BeautifulSoup = BeautifulSoup(req_data.text, "html.parser")
        case_count: str = soup.find("title").text
        print(case_count)
        match_groups: Optional[Match[str]] = re.search(r"[0-9,]+", case_count)
        if match_groups is not None:
            total_case_count: int = int(match_groups.group(0).replace(",", ""))
            cache.set("total_case_count", total_case_count)
        else:
            total_case_count = cache.get("total_case_count")
        print("Soup successful...")
    except AttributeError as e:
        print("should have attribute error....")
        logging.error(f"Could not parse {URL} to get the coronovirus case count...", e)
        return -1

    return total_case_count


@udf(returnType=StringType())  # type: ignore
def process_tweet_udf(tweet: str) -> str:
    url_replaced_tweet: str = re.sub(r"http\S+", "", tweet)
    processed_tweet: str = re.sub(r"[#RT:]+", "", url_replaced_tweet)
    return processed_tweet.strip()
