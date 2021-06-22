import re

from typing import Match, Optional

import requests

from bs4 import BeautifulSoup
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, StringType
from requests import Response
from stream_processor.common import config
from stream_processor.common.utils import get_corona_case_count_from_cache, update_cache


@udf(returnType=IntegerType())  # type: ignore
def get_total_case_count_udf() -> int:
    url: str = config.WORLDOMETER_URL
    try:
        req_data: Response = requests.get(url, timeout=(5, 10))
        soup: BeautifulSoup = BeautifulSoup(req_data.text, "html.parser")
        case_count: str = soup.find("title").text
        match_groups: Optional[Match[str]] = re.search(r"[0-9,]+", case_count)
        if match_groups is not None:
            total_case_count: int = int(match_groups.group(0).replace(",", ""))
            update_cache("total_case_count", total_case_count)
        else:
            total_case_count: int = get_corona_case_count_from_cache("total_case_count")
    except Exception:
        return get_corona_case_count_from_cache("total_case_count")

    return total_case_count


@udf(returnType=StringType())  # type: ignore
def process_tweet_udf(tweet: str) -> str:
    url_replaced_tweet: str = re.sub(r"http\S+", "", tweet)
    processed_tweet: str = re.sub(r"[#RT:]+", "", url_replaced_tweet)
    return processed_tweet.strip()
