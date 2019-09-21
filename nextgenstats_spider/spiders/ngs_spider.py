import scrapy
from scrapy_splash import SplashRequest
from scrapy_selenium import SeleniumRequest

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

import pandas as pd

class NGSSpider(scrapy.Spider):
    name = "ngs_spider"

    def start_requests(self):

        #TODO add logic to handle any table, year and set of week numbers

        urls = ['https://nextgenstats.nfl.com/stats/{}/{}/{}'.format(self.type, self.year, self.week)]

        for url in urls:
            yield SeleniumRequest(
            url=url,
            callback=self.parse,
            wait_time=10,
            wait_until=EC.presence_of_element_located((By.CLASS_NAME, "el-table__row"))
            )


    def parse(self, response):

        # with open('dump.html', 'w') as html_file:
        #     html_file.write(response.text)

        # First, we get the column headers and set up a DataFrame.
        # For some reason the entire table seems to be repeated,
        # so we'll only grab the first instance.
        HEAD_CAT_SELECTOR = '(.//thead)[1]//div[@class="cell"]//text()'
        HEAD_NUM_SELECTOR = '(.//thead)[1]//div[@class="cell tooltip-column"]/span/span[1]/text()'

        head_cat = response.xpath(HEAD_CAT_SELECTOR).getall()
        head_num = response.xpath(HEAD_NUM_SELECTOR).getall()

        output = pd.DataFrame(columns=head_cat+head_num)

        # Next we'll grab the rows of data

        ROW_SELECTOR = '(.//tbody)[1]//tr[@class="el-table__row" or @class="el-table__row el-table__row--striped"]'

        for row in response.xpath(ROW_SELECTOR):

            CELL_SELECTOR = './/div[@class="cell"]//text()'
            cells = row.xpath(CELL_SELECTOR).getall()

            print(cells)

            output.loc[len(output)] = cells

        #print("Column headers:", columns)


    def clean(self, output):
            filename = 'ngs_{}_{}_week_{}'.format(self.type, self.year, self.week)

            output.to_csv('data/{}.csv'.format(filename))
