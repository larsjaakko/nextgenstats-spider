# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import pandas as pd

class NextgenstatsSpiderPipeline(object):


    def open_spider(self, spider):
        self.df = None
        pass

    def close_spider(self, spider):
        self.df.to_csv('data/ngs_{}_{}_{}.csv'.format(
            spider.type,
            spider.year,
            spider.weeks,
            ))

    def process_item(self, item, spider):

        if item['type'] == 'columns' and self.df is None:
            if spider.week != 'all':
                item['cells'].append('week')
                self.df = pd.DataFrame(columns = item['cells'])
            else:
                self.df = pd.DataFrame(columns = item['cells'])
        elif item['type'] == 'rows':
            if spider.week != 'all':
                item['cells'].append(item['week'])
                self.df.loc[len(self.df)] = item['cells']
            else:
                self.df.loc[len(self.df)] = item['cells']


        return item

    def clean_passers(self, spider):
        pass

    def clean_receivers(self, spider):
        pass

    def clean_rushers(self, spider):
        pass
