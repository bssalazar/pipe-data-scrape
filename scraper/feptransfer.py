import uuid
from datetime import date, timedelta
from io import StringIO
import logging

import pandas
from bs4 import BeautifulSoup

from scraper import PipelineScraper


logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)
logger = logging.getLogger(__name__)


class FepTransfer(PipelineScraper):
    tsp = '829416002'
    tsp_name = 'Fayetteville Express Pipeline LLC'
    source = 'feptransfer.energytransfer'
    api_url = 'https://feptransfer.energytransfer.com/index.jsp'
    get_url = 'https://feptransfer.energytransfer.com/ipost/FEP/capacity/operationally-available'
    download_csv_url = 'https://feptransfer.energytransfer.com/ipost/capacity/operationally-available'

    params = {
        'f': 'csv',
        'extension': 'csv',
        'asset': 'FEP',
        'gasDay': date.today().strftime('%m/%d/%Y'),
        'cycle': '1', # 0 = timely, 1 = evening, 3 = ID1, 4 = ID2, 7 = ID3
        'searchType': 'NOM',
        'searchString': '',
        'locType': 'ALL',
        'locZone': 'ALL'
    }

    get_page_headers = {
        'Accept': 'text / html, application / xhtml + xml, application / xml; q = 0.9, image / avif, image / webp, image / apng, * / *;q = 0.8, application / signed - exchange; v = b3; q = 0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US, en; q = 0.9, fil; q = 0.8',
        'Connection': 'keep-alive',
        'Host': 'feptransfer.energytransfer.com',
        'Referer': 'https://feptransfer.energytransfer.com/ipost/FEP/capacity/operationally-available?max=ALL',
        'sec-ch-ua': '"Chromium";v="104","Not A;Brand";v="99","Google Chrome";v="104"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'Sec-Fetch-Dest': 'document',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36'
    }

    def __init__(self, job_id):
        PipelineScraper.__init__(self, job_id, web_url=self.api_url, source=self.source)

    def start_scraping(self, post_date: date = None):
        try:
            logger.info('Scraping %s pipeline gas for post date: %s', self.source, post_date)
            response = self.session.get(self.download_csv_url, headers=self.get_page_headers, params=self.params)
            response.raise_for_status()
            html_text = response.text
            csv_data = StringIO(html_text)
            df_result = pandas.read_csv(csv_data)
            response = self.session.get(self.get_url, headers=self.get_page_headers)
            soup = BeautifulSoup(response.text, 'lxml')
            df_result.insert(0, 'TSP', self.tsp, True)
            df_result.insert(1, 'TSP Name', self.tsp_name, True)
            df_result.insert(2, 'Post Date/Time', soup.find_all('strong')[0].nextSibling.text.strip(), True)
            df_result.insert(3, 'Effective Gas Day/Time', soup.find_all('strong')[1].nextSibling.text.strip(), True)
            df_result.insert(4, 'Meas Basis Desc', soup.find_all('strong')[2].nextSibling.text.strip(), True)
            self.save_result(df_result, post_date=post_date, local_file=True)

            logger.info('File saved. end of scraping: %s', self.source)

        except Exception as ex:
            logger.error(ex, exc_info=True)

        return None


def back_fill_pipeline_date():
    scraper = FepTransfer(job_id=str(uuid.uuid4()))
    for i in range(90, -1, -1):
        post_date = (date.today() - timedelta(days=i))
        print(post_date)
        scraper.start_scraping(post_date)


def main():
    scraper = FepTransfer(job_id=str(uuid.uuid4()))
    scraper.start_scraping()
    scraper.scraper_info()


if __name__ == '__main__':
    main()
