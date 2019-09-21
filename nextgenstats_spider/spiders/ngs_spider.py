import scrapy
from scrapy_splash import SplashRequest
from scrapy_selenium import SeleniumRequest

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

class NGSSpider(scrapy.Spider):
    name = "ngs_spider"

    def start_requests(self):

        #TODO add logic to handle any week numbers

        urls = ['https://nextgenstats.nfl.com/stats/{}/{}/{}'.format(self.type, self.year, self.week)]

        for url in urls:
            yield SeleniumRequest(
            url=url,
            callback=self.parse,
            wait_time=10,
            wait_until=EC.presence_of_element_located((By.CLASS_NAME, "ngs-data-table"))
            )





    def parse(self, response):
        print('RESPONSE: ', response.request.meta['driver'].page_source)
        #print('TABLE:', response.css('ngs-data-table::text'))
        #print('RESPONSE: ', response.body)

        response.request.meta['driver'].get_screenshot_as_file('image_2.png')

        yield {
               'cell': response.css('.cell::text').extract_first(),
            }
