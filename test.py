import locale
import re

import requests

from bs4 import BeautifulSoup


if __name__ == "__main__":
    url = "https://www.worldometers.info/coronavirus/"
    req_data = requests.get(url)
    soup = BeautifulSoup(req_data.text, "html.parser")
    text = soup.find("title").text
    m = re.search(r"[0-9,]+", text).group(0)
    print(locale.atoi(m))
