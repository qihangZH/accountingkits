import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin


def search_html_suburls_list(url, **kwargs):
    response = requests.get(url, **kwargs)

    if response.status_code == 200:

        html_content = response.text

        soup = BeautifulSoup(html_content, "html.parser")

        anchor_tags = soup.find_all("a")

        sub_urls_list = []
        for anchor_tag in anchor_tags:
            href = anchor_tag.get("href")
            absolute_url = urljoin(url, href)
            sub_urls_list.append(absolute_url)

        return sub_urls_list
    else:
        raise Exception(f"Error: Unable to fetch the content. Status code: {response.status_code}")
