import requests
import bs4


def request_url_text(url, **kwargs) -> str:
    """
    extract the texture in an url/website, return string. If you need sleep, set it outside.
    :param url: the url to be request
    :param kwargs: the kwargs of request.get(), which help you to modify and face some situations.
    :return: string of url text
    """
    response = requests.get(url, **kwargs)
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
