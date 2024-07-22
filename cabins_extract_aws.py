import os
import time
import json
import logging
import re
from datetime import datetime
from collections import Counter

import scrapy  # type: ignore
from scrapy.crawler import CrawlerRunner  # type: ignore
from selenium import webdriver  # type: ignore
from selenium.webdriver.common.by import By  # type: ignore
from selenium.webdriver.common.keys import Keys  # type: ignore
from selenium.webdriver.support.ui import WebDriverWait  # type: ignore
from selenium.webdriver.support import expected_conditions as EC  # type: ignore
from twisted.internet import reactor, defer  # type: ignore
import boto3
from dotenv import load_dotenv
import os

# Configuration and Constants
TIME_STAMP = datetime.now().strftime("%Y%m%d-%H%M%S")

FILENAME = f"etuovi_data_{TIME_STAMP}.json"
AWS_BUCKET_NAME = os.getenv('AWS_BUCKET_NAME')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_BUCKET_PATH = f"{AWS_BUCKET_NAME}/etuovi_data/{FILENAME}"


LOGGING_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=LOGGING_FORMAT)


def get_etuovi_url() -> str:
    """Retrieve the URL for cabin listings on Etuovi.com."""
    options = webdriver.FirefoxOptions()
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/x-gzip")

    driver = webdriver.Firefox(options=options)
    wait = WebDriverWait(driver, 100)

    url = 'https://www.etuovi.com/myytavat-loma-asunnot'
    driver.get(url)

    try:
        accept_cookies = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="almacmp-modalConfirmBtn"]')))
        accept_cookies.click()
    except Exception as e:
        logging.warning("No cookies window or unable to click accept button: %s", e)

    time.sleep(10)

    try:
        muokka_hakua = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div/div/div[3]/div/div[2]/div[2]/div[2]/div[2]/div/div[1]/div[2]/div/div/button')))
        muokka_hakua.click()

        time.sleep(10)

        mokki_tai_huvila = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div/div/div[3]/div/div/div[2]/div/div[2]/form/div[1]/div[1]/div/div[1]/div[3]/div[1]/div/div[2]/div[1]/div')))
        driver.execute_script("arguments[0].scrollIntoView(true);", mokki_tai_huvila)
        mokki_tai_huvila.click()

        time.sleep(1)

        jarvi = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div/div/div[3]/div/div/div[2]/div/div[2]/form/div[1]/div[1]/div/div[1]/div[3]/div[3]/div/div[2]/div[1]/div')))
        jarvi.click()

        nayta_ilmoitukset = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="searchButton"]')))
        nayta_ilmoitukset.click()
        webdriver.ActionChains(driver).send_keys(Keys.ESCAPE).perform()

        time.sleep(5)

        etuovi_url = driver.current_url
    finally:
        driver.quit()

    return etuovi_url

etuovi_url = get_etuovi_url()
listing_urls = []

class EtuoviSpider(scrapy.Spider):
    name = "all_listings"
    start_urls = [etuovi_url]

    def parse(self, response):
        elements_with_classes = response.xpath('//*[@class]')
        all_classes = [cls for element in elements_with_classes for cls in element.xpath('@class').get().split()]
        class_counts = Counter(all_classes)

        filtered_classes = [cls for cls, count in class_counts.items() if count == 30 and len(cls) == 7 and cls.isalpha()]
        results = response.css(f'div.{filtered_classes[0]}')

        for r in results:
            url = "https://www.etuovi.com" + r.css('a::attr(href)').get().split("?haku")[0]
            cabin = {
                "address": r.css('h4::text').get(),
                "url": url,
                "metrics": r.css('span::text').getall(),
                "description": r.css('h5::text').get()
            }
            listing_urls.append(url)
            yield cabin

        current_url = response.request.url
        last_page_xpath = '/html/body/div[2]/div/div/div[3]/div/div[2]/div[3]/div[1]/div[1]/div[1]/div[4]/div[1]/div[6]/button'

        try:
            last_page_number = int(response.xpath(last_page_xpath).css('::text').get())
        except Exception as e:
            logging.warning("Unable to retrieve last page: %s", e)
            return

        if re.search("&sivu=[0-9]{1,10}", current_url):
            end = re.search(r'\d+$', current_url)
            index = int(end.group(0)) + 1 if end else 2
            if index <= last_page_number:
                next_page = re.sub("&sivu=[0-9]{1,10}", "", current_url) + f"&sivu={index}"
                yield response.follow(next_page, callback=self.parse)
        else:
            next_page = f"{current_url}&sivu=2"
            yield response.follow(next_page, callback=self.parse)

class ListingsSpider(scrapy.Spider):
    name = "listing_details"

    def __init__(self, urls, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.start_urls = urls

    def parse(self, response):
        winterized = "YES" if float(response.xpath('count(//text()[normalize-space() = "Kohde on talviasuttava"])').extract()[0]) >= 2 else "NO"

        details = {
            "url": response.request.url,
            "rooms": response.xpath('//div[descendant::em[contains(text(), "Huoneita")]]/following-sibling::div[1]//text()').extract_first(),
            "winterized": winterized
        }
        yield details

class CrawlerScript:
    def __init__(self):
        self.filename = FILENAME
        self.settings = {
            'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.79 Safari/537.36',
            'LOG_LEVEL': logging.INFO,
            'DOWNLOAD_DELAY': 3,
            'ROBOTSTXT_OBEY': False,
            'AWS_ACCESS_KEY_ID ': AWS_ACCESS_KEY_ID,
            'AWS_SECRET_ACCESS_KEY ': AWS_SECRET_ACCESS_KEY,
            "FEEDS": {
                AWS_BUCKET_PATH: {
                'format': 'json',
                'encoding': 'utf8',
                'store_empty': False
                        }
            }
        }
        self.runner = CrawlerRunner(self.settings)
        self.listing_data = []

    def run(self):
        @defer.inlineCallbacks
        def crawl():
            yield self.runner.crawl(EtuoviSpider)
            logging.info("The number of listing URLs is: %d", len(listing_urls))
            yield self.runner.crawl(ListingsSpider, urls=listing_urls)
            reactor.stop()

        crawl()
        reactor.run()

if __name__ == "__main__":
    script = CrawlerScript()
    script.run()
