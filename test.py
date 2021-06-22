import locale
import re

import requests

from bs4 import BeautifulSoup
from pymemcache.client.base import Client


if __name__ == "__main__":
    url = "https://www.worldometers.info/coronavirus/"
    req_data = requests.get(url)
    soup = BeautifulSoup(req_data.text, "html.parser")
    text = soup.find("title").text
    match_groups = re.search(r"[$]+", text)
    m = "0"
    client = Client('localhost:11211')
    client.set('total_case_count', "1000")
    if match_groups is not None:
       m = match_groups.group(0)
    else:
       m = client.get('total_case_count')
    print(m.decode("utf-8").replace(",", ""))
