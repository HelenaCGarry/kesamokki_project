import os
from datetime import datetime
import time
import json
import logging
import scrapy # type: ignore
from scrapy.crawler import CrawlerRunner # type: ignore
from selenium import webdriver # type: ignore
from selenium.webdriver.common.by import By # type: ignore
from selenium.webdriver.common.keys import Keys # type: ignore
from selenium.webdriver.support.ui import WebDriverWait # type: ignore
from selenium.webdriver.support import expected_conditions as EC # type: ignore
from twisted.internet import reactor, defer # type: ignore
from collections import Counter
import re

# Get current date for filename


download_dir = os.path.abspath("data/cabins")
time_stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
filename = f"etuovi_data_{time_stamp}.json"
file_path = os.path.join(download_dir, filename)

if filename in os.listdir(download_dir):  
    os.remove(file_path)



# Obtain the webpage for cabin listings on a lake

def get_etuovi_url():
    # Set download preferences for Firefox
    options = webdriver.FirefoxOptions()
    options.set_preference("browser.download.folderList", 2)
    options.set_preference("browser.download.manager.showWhenStarting", False)
    options.set_preference("browser.download.dir", download_dir)
    options.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/x-gzip")

    driver = webdriver.Firefox(options=options)
    wait = WebDriverWait(driver, 100)

    # Define the Etuovi.com intial URL
    url = 'https://www.etuovi.com/myytavat-loma-asunnot'
    driver.get(url)

    # Accept cookies
    try:
        accept_cookies = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="almacmp-modalConfirmBtn"]')))
        accept_cookies.click()
    except Exception as e:
        print("No cookies window or unable to click accept button:", e)

    time.sleep(10)

    # Select "Muokka Hakua" (Modify Search)
    muokka_hakua = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div/div/div[3]/div/div[2]/div[2]/div[2]/div[2]/div/div[1]/div[2]/div/div/button')))
    muokka_hakua.click()

    time.sleep(10)

    # Select "Mökki tai huvila" (Cabin or Villa)
    mokki_tai_huvila = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div/div/div[3]/div/div/div[2]/div/div[2]/form/div[1]/div[1]/div/div[1]/div[3]/div[1]/div/div[2]/div[1]/div')))
    driver.execute_script("arguments[0].scrollIntoView(true);", mokki_tai_huvila)
    mokki_tai_huvila.click()

    time.sleep(1)

    # Select "Järvi" (Shore type: lake)
    jarvi = wait.until(EC.element_to_be_clickable((By.XPATH, '/html/body/div[2]/div/div/div[3]/div/div/div[2]/div/div[2]/form/div[1]/div[1]/div/div[1]/div[3]/div[3]/div/div[2]/div[1]/div')))
    jarvi.click()

    # Select "Näytä ilmoitukset" (Show postings)
    nayta_ilmoitukset = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="searchButton"]')))
    nayta_ilmoitukset.click()
    webdriver.ActionChains(driver).send_keys(Keys.ESCAPE).perform()

    # Wait for the page to load
    time.sleep(5)

    # Get current URL
    etuovi_url = driver.current_url

    # Quit driver
    driver.quit()

    # Return URL
    return etuovi_url

etuovi_url = get_etuovi_url()

# Define first level spyder for scraping listings

class EtuoviSpider(scrapy.Spider):
    name = "all_listings"
    start_urls = [etuovi_url]

    def parse(self, response):
        elements_with_classes = response.xpath('//*[@class]')
        
        # Extract all classes from these elements
        all_classes = []
        for element in elements_with_classes:
            classes = element.xpath('@class').get().split()
            all_classes.extend(classes)

        # Count the frequency of each class
        class_counts = Counter(all_classes)

        # Filter classes based on the given criteria
        filtered_classes = [cls for cls, count in class_counts.items()
                            if count == 30 and len(cls) == 7 and cls.isalpha()]

        results = response.css(f'div.{filtered_classes[0]}')

        for r in results[:]:
            cabin = {
                "address": r.css('h4::text').get(),
                "url": "https://www.etuovi.com" + r.css('a::attr(href)').get().split("?haku")[0],
                "metrics": r.css('span::text').getall(),
                "description": r.css('h5::text').get()
            }
            yield cabin
            
        current_url = response.request.url
        last_page_xpath = '/html/body/div[2]/div/div/div[3]/div/div[2]/div[3]/div[1]/div[1]/div[1]/div[4]/div[1]/div[6]/button'
        
        try:
            last_page_number = int(response.xpath(last_page_xpath).css('::text').get())
        except Exception as e:
            print("Unable to retrieve last page:", e)

        if bool(re.search("&sivu=[0-9]{1,10}", current_url)):
            end = re.search(r'\d+$', current_url)
            index = int(end.group(0)) + 1
            last_page = last_page_number
            if index > int(last_page):
                logging.info('No next page. Terminating crawling process.')
            else:
                next_page = re.sub("&sivu=[0-9]{1,10}", "", current_url) + "&sivu=" + str(index)
                yield response.follow(next_page, callback=self.parse)
        else:
            index = 2
            next_page = current_url + "&sivu=" + str(index)
            yield response.follow(next_page, callback=self.parse)

# Second level spyder for scraping addtional information from each listing

class ListingsSpider(scrapy.Spider):
    name = "listing_details"

    def __init__(self, urls, *args, **kwargs):
        super(ListingsSpider, self).__init__(*args, **kwargs)
        self.start_urls = urls

    def parse(self, response):
        if float(response.xpath('count(//text()[normalize-space() = "Kohde on talviasuttava"])').extract()[0]) >= 2:
            winterized = "YES"
        else:
            winterized = "NO"

        details = {
            "url": response.request.url,
            "rooms": response.xpath('//div[descendant::em[contains(text(), "Huoneita")]]/following-sibling::div[1]//text()').extract()[0],
            "winterized": winterized
        }
        yield details

# Crawler script including both levels of spyder

class CrawlerScript:
    def __init__(self):
        self.filename = filename
        self.settings = {
            'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.79 Safari/537.36',
            'LOG_LEVEL': logging.INFO,
            'DOWNLOAD_DELAY': 3,
            'ROBOTSTXT_OBEY': False,
            "FEEDS": {
                file_path: {"format": "json"}
            }
        }
        self.runner = CrawlerRunner(self.settings)
        self.listing_data = []

    def run(self):
        @defer.inlineCallbacks
        def crawl():
            yield self.runner.crawl(EtuoviSpider)
            with open(file_path) as f:
                self.listing_data = json.load(f)
            urls = [listing['url'] for listing in self.listing_data]
            print("############################")
            print("The number of listing urls is:")
            print(len(urls))
            print("############################")
            yield self.runner.crawl(ListingsSpider, urls=urls)
            reactor.stop()

        crawl()
        reactor.run()

if __name__ == "__main__":
    script = CrawlerScript()
    script.run()