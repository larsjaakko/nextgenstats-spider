import scrapy
from scrapy_selenium import SeleniumRequest

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

import re

class NGSSpider(scrapy.Spider):
    name = "ngs_spider"

    def __init__(self, week='all', year='', type='', **kwargs):
        super().__init__(**kwargs)
        self.week = week
        self.year = year
        self.type = type


    def start_requests(self):

        #TODO add logic to handle any table, year and set of week numbers

        base = 'https://nextgenstats.nfl.com/stats/{}/{}/{}'

        self.week_list, self.weeks = self.parse_weeks()

        urls = {self.week_list[i]: base.format(self.type, self.year, self.week_list[i]) for i, j in enumerate(self.week_list)}

        print('URLS: ', urls)

        for week, url in urls.items():
            yield SeleniumRequest(
                url=url,
                callback=self.parse,
                wait_time=10,
                wait_until=EC.presence_of_element_located((By.CLASS_NAME, "el-table__row")),
                meta={'week': week}
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

        columns = head_cat + head_num

        yield{
            'type' : 'columns',
            'cells' :  columns,
            'week' : response.meta['week']
        }

        # Next we'll grab the rows of data

        ROW_SELECTOR = '(.//tbody)[1]//tr[@class="el-table__row" or @class="el-table__row el-table__row--striped"]'

        for row in response.xpath(ROW_SELECTOR):

            CELL_SELECTOR = './/div[@class="cell"]//text()'
            cells = row.xpath(CELL_SELECTOR).getall()

            yield{
                'type' : 'rows',
                'cells' :  cells,
                'week' : response.meta['week'],
            }

    def parse_weeks(self):

        if self.week == 'all' or self.week is None:
            return ['all'], 'all'
        elif self.week == 'post':
            return list(range(18,23)), 'post'
        elif self.week == 'reg':
            return list(range(1,18)), 'reg'
        elif self.week.isdigit() and (int(self.week) <= 1 and int(self.week) <= 17):
            return [self.week], self.week
        elif ':' in self.week:
            interval = self.week.split(':')
            interval = list(map(int, interval))
            interval[1] += 1
            return list(range(*interval)), self.week.replace(':', '_to_')
        elif ',' in self.week:
            return self.week.split(','), self.week.replace(',', '_')
        else:
            raise Exception('Week parameter was incorrectly given.')
