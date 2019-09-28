import scrapy
from scrapy import Selector
from scrapy_selenium import SeleniumRequest

import time

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.firefox.options import Options

try:
    from tenacity import *
except ImportError:
    pass

class NGSSpider(scrapy.Spider):
    name = "ngs_spider"

    def __init__(self, week='reg', year='', type='', ids=False, **kwargs):
        super().__init__(**kwargs)
        self.week = week

        if ids.lower() == 'true':
            self.ids = True
        else:
            self.ids = False

        if year == '':
            raise Exception('You have to specify the year.')
        else:
            self.year = year

        if type == '' or type not in ['passing', 'rushing', 'receiving', 'fastest']:
            raise Exception('Type missing or incorrect.')
        else:
            self.type = type


    def start_requests(self):

        self.week_list, self.weeks = self.parse_weeks()

        if self.type != 'fastest':
            base = 'https://nextgenstats.nfl.com/stats/{}/{}/{}'
            urls = {self.week_list[i]: base.format(
                self.type,
                self.year,
                self.week_list[i]) for i, j in enumerate(self.week_list)}
        else:
            base = 'https://nextgenstats.nfl.com/stats/top-plays/fastest-ball-carriers/{}/{}'
            urls = {self.week_list[i]: base.format(
                self.year,
                self.week_list[i]) for i, j in enumerate(self.week_list)}


        for week, url in urls.items():
            yield SeleniumRequest(
                url=url,
                callback=self.parse,
                wait_time=10,
                wait_until=EC.presence_of_element_located((By.CLASS_NAME, "el-table__row")),
                meta={
                    'week': week,
                    'page' : url}
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
            'week' : response.meta['week'],
            'url' : response.meta['page']
        }

        # Next we'll grab the rows of data

        ROW_SELECTOR = '(.//tbody)[1]//tr[@class="el-table__row" or @class="el-table__row el-table__row--striped"]'

        for i, row in enumerate(response.xpath(ROW_SELECTOR)):

            CELL_SELECTOR = './/div[@class="cell"]//text()[not(ancestor::i)]'
            cells = row.xpath(CELL_SELECTOR).getall()

            self.logger.info('Parsing: {}'.format(cells))

            yield{
                'type' : 'rows',
                'cells' :  cells,
                'week' : response.meta['week'],
                'url' : response.meta['page']
            }

        if self.type == 'fastest' and self.ids == True:

            descriptions = self.fetch_descriptions(response)

            yield{
                'type' : 'descriptions',
                'cells' :  descriptions,
                'week' : response.meta['week'],
                'url' : response.meta['page']
            }

            self.logger.info('Parsing: {}'.format(descriptions))



    def parse_weeks(self):

        if self.week == 'all':
            return ['all'], 'overall'
        elif self.week == 'post':
            return [18, 19, 20, 22], 'post'
        elif self.week == 'reg' or self.week is None:
            return list(range(1,18)), 'reg'
        elif self.week.isdigit() and (int(self.week) >= 1 and int(self.week) <= 22):
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


    @retry(stop=stop_after_attempt(5), wait=wait_fixed(2))
    def fetch_descriptions(self, response):

        options = Options()
        options.headless = True

        driver = webdriver.Firefox(options=options)
        driver.get(response.url)
        WebDriverWait(driver, 20).until(EC.presence_of_element_located((By.CLASS_NAME, "el-table__row")))

        driver.find_element_by_xpath('//a[@aria-label="dismiss cookie message"]').click()
        time.sleep(3)

        BUTTON_SELECTOR ='(.//tbody)[1]//button[@class="v-btn v-btn--flat theme--light"]'
        CLOSE_SELECTOR ='//div[@class="v-dialog v-dialog--active"]//button[@class="green--text darken-1 v-btn v-btn--flat theme--light"]'

        buttons = driver.find_elements_by_xpath(BUTTON_SELECTOR)

        descriptions = []

        WebDriverWait(driver, 20).until(EC.element_to_be_clickable((By.XPATH, '//*[@id="stats-top-plays-view"]/div[3]/div/div[3]/table/tbody/tr[1]/td[6]/div/button')))
        for button in buttons:

            WebDriverWait(driver, 20).until(EC.invisibility_of_element_located((By.CLASS_NAME, 'cc-window cc-banner cc-type-info cc-theme-block cc-bottom cc-color-override-382972913 ')))
            WebDriverWait(driver, 20).until(EC.invisibility_of_element_located((By.CLASS_NAME, 'cc-window cc-banner cc-type-info cc-theme-block cc-bottom cc-color-override-382972913  cc-invisible')))
            button.click()

            self.logger.info('---- CLICKED BUTTON ----')
            time.sleep(1)

            sel = Selector(text=driver.page_source)
            description = sel.xpath('//div[@class="v-dialog v-dialog--active"]//div[@class="v-card__text"]/p//text()').extract_first()

            descriptions.append(description)

            self.logger.info('Added description: {}'.format(description))

            WebDriverWait(driver, 20).until(EC.invisibility_of_element_located((By.CLASS_NAME, 'cc-window cc-banner cc-type-info cc-theme-block cc-bottom cc-color-override-382972913 ')))
            WebDriverWait(driver, 20).until(EC.invisibility_of_element_located((By.CLASS_NAME, 'cc-window cc-banner cc-type-info cc-theme-block cc-bottom cc-color-override-382972913  cc-invisible')))

            driver.find_element_by_xpath(CLOSE_SELECTOR).click()
            self.logger.info('---- CLOSED POPUP ----')
            time.sleep(1)


        driver.quit()

        return descriptions
