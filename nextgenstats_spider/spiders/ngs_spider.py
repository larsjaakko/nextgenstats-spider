import scrapy
from scrapy_selenium import SeleniumRequest

from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC

class NGSSpider(scrapy.Spider):
    name = "ngs_spider"

    def __init__(self, week='reg', year='', type='', **kwargs):
        super().__init__(**kwargs)
        self.week = week

        if year == '':
            raise Exception('You have to specify the year.')
        else:
            self.year = year

        if type == '' or type not in ['passing', 'rushing', 'receiving', 'fastest']:
            raise Exception('Type missing or incorrect.')
        elif type == 'fastest':
            self.type = 'fastest-ball-carriers'
        else:
            self.type = type


    def start_requests(self):

        if self.type != 'fastest-ball-carriers':
            base = 'https://nextgenstats.nfl.com/stats/{}/{}/{}'
        else:
            base = 'https://nextgenstats.nfl.com/stats/top-plays/{}/{}/{}'

        self.week_list, self.weeks = self.parse_weeks()

        urls = {self.week_list[i]: base.format(
            self.type,
            self.year,
            self.week_list[i]) for i, j in enumerate(self.week_list)}

        for week, url in urls.items():
            yield SeleniumRequest(
                url=url,
                callback=self.parse,
                wait_time=10,
                wait_until=EC.presence_of_element_located((By.CLASS_NAME, "el-table__row")),
                meta={'week': week}
            )




    def parse(self, response):

        # First, we get the column headers
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

            CELL_SELECTOR = './/div[@class="cell"]//text()[not(ancestor::i)]'
            cells = row.xpath(CELL_SELECTOR).getall()
            self.logger.info('Parsing: {}'.format(cells))

            yield{
                'type' : 'rows',
                'cells' :  cells,
                'week' : response.meta['week'],
            }


    def parse_weeks(self):

        if self.week == 'all':
            return ['all'], 'overall'
        elif self.week == 'post':
            return list(range(18,23)), 'post'
        elif self.week == 'reg' or self.week is None:
            return list(range(1,18)), 'reg'
        elif self.week.isdigit() and (int(self.week) >= 1 and int(self.week) <= 17):
            return [self.week], self.week
        elif ':' in self.week:
            interval = self.week.split(':')
            interval = list(map(int, interval))
            if interval[0] > interval[1]:
                raise Exception('For a week range, make sure the first week happens before the last...')
            interval[1] += 1
            return list(range(*interval)), self.week.replace(':', '_to_')
        elif ',' in self.week:
            return self.week.split(','), self.week.replace(',', '_')
        else:
            raise Exception('Week parameter was incorrectly given.')
