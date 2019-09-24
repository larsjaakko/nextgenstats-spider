# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import pandas as pd
import logging

COL_NAMES_PASS = {

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

COL_ORDER_PASS = [
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

COL_NAMES_REC = {
    'PLAYER NAME' : 'playerName',
    'TEAM' : 'team',
    'POS' : 'position',
    'CUSH' : 'averageCushion',
    'SEP' : 'averageSeparation',
    'TAY' : 'averageTargetedAirYards',
    'TAY%' : 'shareOfTeamAirYards',
    'REC' : 'receptions',
    'TAR' : 'targets',
    'CTCH%' : 'catchPercentage',
    'YDS' : 'receivingYards',
    'TD' : 'receivingTouchdowns',
    'YAC/R' : 'avgYAC',
    'xYAC/R' : 'expectedAvgYAC',
    '+/-' : 'avgYACAboveExpectation'
    }

COL_ORDER_REC = [
    'shortName',
    'playerName',
    'team',
    'position',
    'averageCushion',
    'averageSeparation',
    'averageTargetedAirYards',
    'shareOfTeamAirYards',
    'receptions',
    'targets',
    'catchPercentage',
    'receivingYards',
    'receivingTouchdowns',
    'avgYAC',
    'expectedAvgYAC',
    'avgYACAboveExpectation',
    'season',
    'seasonType',
    'week'
    ]

COL_NAMES_RUSH =  {
    'PLAYER NAME' : 'playerName',
    'TEAM' : 'team',
    'EFF' : 'efficiency',
    '8+D%' : '8+_defendersInTheBox',
    'TLOS' : 'avgTimeBehindLineOfScrimmage',
    'ATT' : 'rushingAttempts',
    'YDS' : 'rushingYards',
    'AVG' : 'averageRushYards',
    'TD' : 'rushingTouchdowns',
}

COL_ORDER_RUSH =  [
    'shortName',
    'playerName',
    'team',
    'efficiency',
    '8+_defendersInTheBox',
    'avgTimeBehindLineOfScrimmage',
    'rushingAttempts',
    'rushingYards',
    'averageRushYards',
    'rushingTouchdowns',
    'season',
    'seasonType',
    'week'
]

COL_NAMES_FASTEST = {

    'shortName':'shortName',
    'PLAYER NAME': 'playerName',
    'TEAM': 'team',
    'POS': 'position',
    'Wk': 'week',
    'Speed (MPH)': 'speedMPH'
    }

COL_ORDER_FASTEST =  [
    'shortName',
    'playerName',
    'team',
    'position',
    'speedMPH',
    'yards',
    'playType',
    'touchdown',
    'penalty',
    'season',
    'seasonType',
    'week'
]

class NextgenstatsSpiderPipeline(object):


    def open_spider(self, spider):
        self.df = None
        pass

    def close_spider(self, spider):

        self.df = self.clean_data(spider)

        self.df.to_csv('data/{}/ngs_{}_{}_{}.csv'.format(
            spider.type,
            spider.type,
            spider.year,
            spider.weeks,
            ))

        spider.logger.info('Wrote .csv to data/{}/ngs_{}_{}_{}.csv'.format(
            spider.type,
            spider.type,
            spider.year,
            spider.weeks,
            ))

    def process_item(self, item, spider):

        if spider.type != 'fastest-ball-carriers':

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
        else:

            if item['type'] == 'columns' and self.df is None:
                    self.df = pd.DataFrame(columns = item['cells'])
            elif item['type'] == 'rows':
                    self.df.loc[len(self.df)] = item['cells']

        return item

    def clean_data(self, spider):

        self.df['shortName'] = self.df['PLAYER NAME'].apply(self.name_shortener)

        if spider.type == 'fastest-ball-carriers':
            self.df['Play Type'] = self.df['Play Type'].apply(self.space_remover)
            self.df['yards'] = self.df['Play Type'].apply(lambda x: x.split()[0])
            self.df['playType'] = self.df['Play Type'].apply(lambda x: x.split()[2] if 'ret' not in (x.split()) else ' '.join(x.split()[2:4]))
            self.df['touchdown'] = self.df['Play Type'].apply(lambda x: 1 if "TD" in x.split() else 0)
            self.df['penalty'] = self.df['Play Type'].apply(lambda x: 1 if "*" in x.split() else 0)
            self.df = self.df.drop(['Play Type'], axis=1)

        if spider.type == 'passing':
            self.df = self.df.rename(columns=COL_NAMES_PASS)
        elif spider.type == 'receiving':
            self.df = self.df.rename(columns=COL_NAMES_REC)
        elif spider.type == 'rushing':
            self.df = self.df.rename(columns=COL_NAMES_RUSH)
        elif spider.type == 'fastest-ball-carriers':
            self.df = self.df.rename(columns=COL_NAMES_FASTEST)

        if spider.week != 'all' or spider.type == 'fastest-ball-carriers':
            self.df['seasonType'] = self.df['week'].apply(self.season_type)

        self.df['season'] = spider.year
        #Changing LA Rams abbreviation to match nflscrapR
        self.df[self.df['team'] == 'LAR']['team'] = 'LA'

        if spider.week != 'all':

            if spider.type == 'passing':
                self.df = self.df[COL_ORDER_PASS]
            elif spider.type == 'receiving':
                self.df = self.df[COL_ORDER_REC]
            elif spider.type == 'rushing':
                self.df = self.df[COL_ORDER_RUSH]
            elif spider.type == 'fastest-ball-carriers':
                self.df = self.df[COL_ORDER_FASTEST]
        else:
            if spider.type == 'passing':
                COL_ORDER_PASS.remove('week')
                COL_ORDER_PASS.remove('seasonType')
                self.df = self.df[COL_ORDER_PASS]
            elif spider.type == 'receiving':
                COL_ORDER_REC.remove('week')
                COL_ORDER_REC.remove('seasonType')
                self.df = self.df[COL_ORDER_REC]
            elif spider.type == 'rushing':
                COL_ORDER_RUSH.remove('week')
                COL_ORDER_RUSH.remove('seasonType')
                self.df = self.df[COL_ORDER_RUSH]
            elif spider.type == 'fastest-ball-carriers':
                self.df = self.df[COL_ORDER_FASTEST]

        try:
            self.df['week'] = self.df['week'].astype('int32')
            self.df = self.df.sort_values(by=['week', 'playerName'], ascending=True)
        except:
            pass

        return self.df.reset_index(drop=True)

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

    def space_remover(self, string):

        return " ".join(string.split())


    def season_type(self, week):

        if int(week) <= 17:
            return 'REG'
        elif int(week) >= 18:
            return 'POST'
