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

        if spider.type == 'passing':
            self.df = self.clean_passing(spider)
        elif spider.type == 'rushing':
            self.df = self.clean_rushing()
        elif spider.type == 'receiving':
            self.df = self.clean_receiving()

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

    def clean_passing(self, spider):

        self.df['shortName'] = self.df['PLAYER NAME'].apply(self.name_shortener)

        col_names = {

            'shortName':'shortName',
            'PLAYER NAME': 'playerName',
            'TEAM': 'team',
            'AGG%': 'aggressiveness',
            'ATT': 'attempts',
            'AYD': 'avgAirYardsDifferential',
            'AYTS': 'avgAirYardsToSticks',
            'CAY': 'avgCompletedAirYards',
            'IAY': 'avgIntendedAirYards',
            'TT':'avgTimeToThrow',
            'COMP%':'completionPercentage',
            '+/-':'completionPercentageAboveExpectation',
            'xCOMP%':'expectedCompletionPercentage',
            'INT':'interceptions',
            'LCAD':'maxCompletedAirDistance',
            'TD':'passTouchdowns',
            'YDS':'passYards',
            'RATE':'passerRating',
        }

        col_order = [
            'shortName',
            'playerName',
            'team',
            'aggressiveness',
            'attempts',
            'avgAirYardsDifferential',
            'avgAirYardsToSticks',
            'avgCompletedAirYards',
            'avgIntendedAirYards',
            'avgTimeToThrow',
            'completionPercentage',
            'completionPercentageAboveExpectation',
            'expectedCompletionPercentage',
            'interceptions',
            'maxCompletedAirDistance',
            'passTouchdowns',
            'passYards',
            'passerRating',
            'season',
            'seasonType',
            'week'
        ]

        self.df = self.df.rename(columns=col_names)

        if spider.week != 'all':
            self.df['seasonType'] = self.df['week'].apply(self.season_type)

        self.df['season'] = spider.year

        self.df = self.df[col_order]

        return self.df

    def clean_receiving(self, spider):
        pass

    def clean_rushing(self, spider):
        pass

    def name_shortener(self, name):
        # split the string into a list
        names = name.split()
        short = ''

        # traverse in the list
        for i in range(len(names)-1):
            name = names[i]

            # adds the capital first character
            short += (name[0].upper()+'.')

        # l[-1] gives last item of list l. We
        # use title to print first character in
        # capital.
        short += names[-1].title()

        return short

    def season_type(self, week):

        if week <= 17:
            return 'REG'
        elif week >= 18:
            return 'POST'
