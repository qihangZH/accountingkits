import requests
import bs4


def request_url_text(url) -> str:
    """
    extract the texture in url/website, return string.
    :param url: the url to be request
    """
    response = requests.get(url)
    if response.status_code == 200:
        soup = bs4.BeautifulSoup(response.content, 'html.parser')

        # Remove script and style elements
        for script in soup(['script', 'style']):
            script.extract()

        # Extract text
        text = soup.get_text()

        return text
    else:
        print(f"Error: Unable to fetch the content. Status code: {response.status_code}")
