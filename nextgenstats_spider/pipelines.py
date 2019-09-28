# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import pandas as pd
from collections import defaultdict
import logging

try:
    import requests
except ImportError:
    pass



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
    '8+D%' : '8+DefendersInTheBox',
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
    '8+DefendersInTheBox',
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

        self.datadict = defaultdict(pd.DataFrame)
        self.df = None

    def close_spider(self, spider):

        for k, v in self.datadict.items():
            self.df = self.df.append(v)

        self.df = self.df.reset_index()

        self.df = self.clean_data(spider)

        if spider.type != 'fastest':

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
        else:
            type = 'fastest-ball-carriers'
            self.df.to_csv('data/{}/ngs_{}_{}_{}.csv'.format(
                type,
                type,
                spider.year,
                spider.weeks,
                ))

            spider.logger.info('Wrote .csv to data/{}/ngs_{}_{}_{}.csv'.format(
                type,
                type,
                spider.year,
                spider.weeks,
                ))

    def process_item(self, item, spider):

        if spider.type != 'fastest':

            if item['type'] == 'columns':
                if spider.week != 'all':
                    item['cells'].append('week')
                    self.df = pd.DataFrame(columns = item['cells'])
                    self.datadict[item['url']] = self.datadict[item['url']].reindex(columns=item['cells'])
                else:
                    self.df = pd.DataFrame(columns = item['cells'])
                    self.datadict[item['url']] = self.datadict[item['url']].reindex(columns=item['cells'])

            elif item['type'] == 'rows':
                if spider.week != 'all':
                    item['cells'].append(int(item['week']))
                    self.datadict[item['url']].loc[len(self.datadict[item['url']])] = item['cells']
                else:
                    self.datadict[item['url']].loc[len(self.datadict[item['url']])] = item['cells']
        else:

            if item['type'] == 'columns':
                if spider.week != 'all':
                    self.df = pd.DataFrame(columns = item['cells'])
                    self.datadict[item['url']] = self.datadict[item['url']].reindex(columns=item['cells'])
                else:
                    self.df = pd.DataFrame(columns = item['cells'])
                    self.datadict[item['url']] = self.datadict[item['url']].reindex(columns=item['cells'])
            elif item['type'] == 'rows':
                self.datadict[item['url']].loc[len(self.datadict[item['url']])] = item['cells']
            elif item['type'] == 'descriptions':
                self.datadict[item['url']]['desc'] = item['cells']

        return item

    def clean_data(self, spider):

        self.df['shortName'] = self.df['PLAYER NAME'].apply(self.name_shortener)
        self.df = self.df.replace('--', '')

        if spider.type == 'passing':
            self.df = self.df.rename(columns=COL_NAMES_PASS)
        elif spider.type == 'receiving':
            self.df = self.df.rename(columns=COL_NAMES_REC)
        elif spider.type == 'rushing':
            self.df = self.df.rename(columns=COL_NAMES_RUSH)
        elif spider.type == 'fastest':
            self.df = self.df.rename(columns=COL_NAMES_FASTEST)

        if spider.week != 'all' or spider.type == 'fastest':
            self.df['seasonType'] = self.df['week'].apply(self.season_type)

        if spider.type == 'fastest':

            self.df['Play Type'] = self.df['Play Type'].apply(self.space_remover)
            self.df['yards'] = self.df['Play Type'].apply(lambda x: x.split()[0])
            self.df['playType'] = self.df['Play Type'].apply(lambda x: x.split()[2] if 'ret' not in (x.split()) else ' '.join(x.split()[2:4]))
            self.df['touchdown'] = self.df['Play Type'].apply(lambda x: 1 if "TD" in x.split() else 0)
            self.df['penalty'] = self.df['Play Type'].apply(lambda x: 1 if "*" in x.split() else 0)
            self.df = self.df.drop(['Play Type'], axis=1)

            try:
                self.df['desc'] = self.df['desc'].apply(lambda x: x[3:])
            except:
                spider.logger.info('Failed to prune a play description.')

        self.df['season'] = spider.year
        self.df.loc[self.df['team'] == 'LAR', ['team']] = 'LA'

        #Pulling NFL game IDs
        if spider.ids == True:
            self.df = self.pull_ids(spider)

            COL_ORDER_PASS.append('gameId')
            COL_ORDER_REC.append('gameId')
            COL_ORDER_RUSH.append('gameId')
            COL_ORDER_FASTEST.extend(['gameId', 'playId', 'desc'])

        if spider.week == 'all':

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
            elif spider.type == 'fastest':
                self.df = self.df[COL_ORDER_FASTEST]
        else:

            if spider.type == 'passing':
                self.df = self.df[COL_ORDER_PASS]
            elif spider.type == 'receiving':
                self.df = self.df[COL_ORDER_REC]
            elif spider.type == 'rushing':
                self.df = self.df[COL_ORDER_RUSH]
            elif spider.type == 'fastest':
                self.df = self.df[COL_ORDER_FASTEST]


        try:
            self.df['week'] = self.df['week'].astype('int32')
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

    def clean_descriptions(self):

        self.df['desc'] = apply(lambda x: x.replace('BLT', 'BAL'))
        self.df['desc'] = apply(lambda x: x.replace('LAR', 'LA'))
        self.df['desc'] = apply(lambda x: x.replace('HST', 'HOU'))
        self.df['desc'] = apply(lambda x: x.replace('CLV', 'CLE'))
        self.df['desc'] = apply(lambda x: x.replace('ARZ', 'ARI'))

        self.df['desc'] = apply(lambda x: x.replace('pushed ob at', 'to'))
        self.df['desc'] = apply(lambda x: x.replace('ran ob at', 'to'))

        return self.df



    def pull_ids(self, spider):

        #Making sure week numbers are ints
        self.df['week'] = self.df['week'].astype('int32')
        spider.week_list = [int(i) for i in spider.week_list]

        if spider.week == 'all':
            return self.df

        if min(spider.week_list) <= 17:
            #make call to shcedule feed
            try:
                response_reg = requests.get('http://www.nfl.com/feeds-rs/schedules/{}.json'.format(spider.year)).json()
            except requests.exceptions.RequestException as e:  # This is the correct syntax
                print(e)

            schedules = pd.DataFrame.from_dict(response_reg['gameSchedules'])
            schedules = schedules.loc[schedules['seasonType'] == 'REG', ['gameId', 'week', 'homeTeamAbbr', 'visitorTeamAbbr']]

            schedules = pd.concat([
                schedules.rename(columns={'homeTeamAbbr': 'team'}),
                schedules.rename(columns={'visitorTeamAbbr': 'team'})
                ])

            schedules = schedules.replace('SD', 'LAC')

            self.df = self.df.astype(str).merge(schedules.astype(str), how='left', on=['week', 'team'])


        if max(spider.week_list) >= 18:
            #make calls to score feeds for each week in the postseason
            post_weeks = [i for i in spider.week_list if i >= 18]

            responses = []
            rows = []
            columns = ['week', 'gameId', 'homeTeamAbbr', 'visitorTeamAbbr']

            for i, week in enumerate(post_weeks):
                try:
                    responses.append(requests.get('http://www.nfl.com/feeds-rs/scores/{}/POST/{}.json'.format(spider.year, week)).json())
                    responses[i] = responses[i]['gameScores']
                except requests.exceptions.RequestException as e:  # This is the correct syntax
                    print(e)

                gamescores = responses[i]

                for j, game in enumerate(gamescores):

                    row = []
                    row.append(game['gameSchedule']['week'])
                    row.append(game['gameSchedule']['gameId'])
                    row.append(game['gameSchedule']['homeTeamAbbr'])
                    row.append(game['gameSchedule']['visitorTeamAbbr'])

                    rows.append(row)

            schedules = pd.DataFrame(columns=columns, data=rows)
            schedules = schedules.replace('SD', 'LAC')

            schedules = pd.concat([
                schedules.rename(columns={'homeTeamAbbr': 'team'}),
                schedules.rename(columns={'visitorTeamAbbr': 'team'})
                ])



            self.df = self.df.astype(str).merge(schedules.astype(str), how='left', on=['week', 'team'])

        if spider.type == 'fastest':

            games = self.df['gameId'].unique().tolist()

            columns = ['gameId', 'playId', 'desc']
            rows = []

            for i, game in enumerate(games):

                try:
                    response = requests.get('http://www.nfl.com/liveupdate/game-center/{}/{}_gtd.json'.format(game, game)).json()
                    drives = response['{}'.format(game)]['drives']
                    drives.pop('crntdrv')
                except requests.exceptions.RequestException as e:  # This is the correct syntax
                    print(e)

                for j, drive in drives.items():

                    plays = drive['plays']

                    for k, play in plays.items():

                        row = []
                        row.append(game)
                        row.append(k)

                        desc = plays[k]['desc']
                        row.append(desc)

                        rows.append(row)



            schedules = pd.DataFrame(columns=columns, data=rows)
            schedules.to_csv('debug.csv')

            self.df = self.df.astype(str).merge(schedules.astype(str), how='left', on=['gameId', 'desc'])

        return self.df
