# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import pandas as pd
from collections import defaultdict
import logging
import re

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
                self.df = pd.DataFrame(columns = item['cells'])
                self.datadict[item['url']] = self.datadict[item['url']].reindex(columns=item['cells'])
            elif item['type'] == 'rows':
                self.datadict[item['url']].loc[len(self.datadict[item['url']])] = item['cells']
            elif item['type'] == 'descriptions':
                self.datadict[item['url']]['desc'] = item['cells']

        return item

    def clean_data(self, spider):

        if spider.type == 'fastest':
            self.df = self.digit_remover(self.df)

        self.df['shortName'] = self.df['PLAYER NAME'].apply(self.name_shortener)

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

            if spider.ids == True:
                self.df = self.parse_descriptions(self.df)



        self.df['season'] = spider.year
        self.df.loc[self.df['team'] == 'LAR', ['team']] = 'LA'

        #Pulling NFL game IDs
        if spider.ids == True:
            self.df = self.pull_ids(spider)
            COL_ORDER_FASTEST.extend(['gameId', 'playId', 'quarter', 'time', 'desc'])

            if spider.week != 'all':

                COL_ORDER_PASS.append('gameId')
                COL_ORDER_REC.append('gameId')
                COL_ORDER_RUSH.append('gameId')


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
            self.df['week'] = self.df['week'].astype('int')
        except:
            pass

        self.df = self.df.replace('--', '')

        return self.df.reset_index(drop=True)

    def digit_remover(self, df):

        #Removes rank number and point from fastest ball carrier name_shortener
        df['PLAYER NAME'] = df['PLAYER NAME'].apply(lambda x: ''.join(i for i in x if not i.isdigit()))
        df['PLAYER NAME'] = df['PLAYER NAME'].apply(lambda x: x.replace('. ', ''))

        return df


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
        short += names[-1]

        return short

    def space_remover(self, string):

        return " ".join(string.split())


    def season_type(self, week):

        if int(week) <= 17:
            return 'REG'
        elif int(week) >= 18:
            return 'POST'

    def parse_descriptions(self, df):

        df['quarter'] = self.df['desc'].apply(lambda x: '' if x is None else x[1])

        df['time'] = self.df['desc'].apply(lambda x: re.findall('\((.*?)\)',x))
        df['time'] = self.df['time'].apply(lambda x: '--' if x == [] else x[0])

        df['time'] = self.df['time'].apply(lambda x: '0' + x if len(x) == 4 else x)
        df['time'] = self.df['time'].apply(lambda x: '00' + x if x[0] == ':' else x)


        df['desc'] = df['desc'].apply(lambda x: x.replace('BLT', 'BAL'))
        df['desc'] = df['desc'].apply(lambda x: x.replace('LAR', 'LA'))
        df['desc'] = df['desc'].apply(lambda x: x.replace('HST', 'HOU'))
        df['desc'] = df['desc'].apply(lambda x: x.replace('CLV', 'CLE'))
        df['desc'] = df['desc'].apply(lambda x: x.replace('ARZ', 'ARI'))

        #To help if folks want to join with nflscrapR descriptions:
        df['desc'] = df['desc'].apply(lambda x: x[3:])
        df.loc[df['playType'] == 'kickoff ret', ['time']] = ''

        return df



    def pull_ids(self, spider):

        if spider.week == 'all' and spider.type != 'fastest':
            return self.df

        spider.logger.info('Fetching game IDs and play-by-play data from NFL.')

        #Making sure week numbers are ints
        self.df['week'] = self.df['week'].astype('int32')

        if spider.week == 'all':
            spider.week_list = list(range(1,18))
        else:
            spider.week_list = [int(i) for i in spider.week_list]

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
                ], sort=True)

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
                ], sort=True)



            self.df = self.df.astype(str).merge(schedules.astype(str), how='left', on=['week', 'team'])

        if spider.type == 'fastest':

            games = self.df['gameId'].unique().tolist()

            columns = ['gameId', 'drive', 'playId', 'time', 'quarter', 'touchdown', 'json_desc', 'statId', 'playType', 'yards', 'shortName']
            rows = []

            stat_ids = {
                25 : 'int',
                26 : 'int',
                21 : 'reception',
                22 : 'reception',
                10 : 'rush',
                11 : 'rush',
                33 : 'punt ret',
                34 : 'punt ret',
                39 : 'punt ret',
                40 : 'punt ret',
                45 : 'kickoff ret',
                46 : 'kickoff ret',
                59 : 'misc',
                60 : 'misc',
                63 : 'misc',
                64 : 'misc',
            }

            for i, game in enumerate(games):

                try:
                    response = requests.get('http://www.nfl.com/liveupdate/game-center/{}/{}_gtd.json'.format(game, game)).json()
                    drives = response['{}'.format(game)]['drives']
                    drives.pop('crntdrv')
                except requests.exceptions.RequestException as e:
                    print(e)

                for j, drive in drives.items():

                    plays = drive['plays']

                    for k, play in plays.items():

                        row = []

                        row.append(game)
                        row.append(j)
                        row.append(k)

                        if plays[k]['note'] == 'KICKOFF' or 'kicks' in plays[k]['desc']:
                            row.append('')
                        else:
                            row.append(plays[k]['time'])

                        row.append(plays[k]['qtr'])
                        row.append(plays[k]['sp'])
                        row.append(plays[k]['desc'])

                        players = play['players']

                        for l, player in players.items():

                            for m, stat in enumerate(player):

                                if stat['statId'] in stat_ids.keys():

                                    _row = row.copy()

                                    _row.append(str(stat['statId']))
                                    _row.append(stat_ids[stat['statId']]) # playType
                                    _row.append(str(int(stat['yards'])))
                                    _row.append(stat['playerName'])

                                    rows.append(_row)

            schedules = pd.DataFrame(columns=columns, data=rows)
            schedules = schedules[['gameId', 'drive', 'playId', 'time', 'quarter', 'touchdown', 'json_desc', 'statId', 'playType', 'yards']]
            schedules['yards'] = schedules['yards'].astype('int')

            schedules.sort_values(['gameId', 'quarter', 'time'], ascending=True).to_csv('debug.csv')

            self.df = self.df.astype(str).merge(schedules.astype(str), how='left', on=['gameId', 'quarter', 'time', 'yards', 'touchdown', 'playType'], sort=False)

        return self.df
