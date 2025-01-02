from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
import datetime
import argparse
import time
import re
import os

import urllib.parse
import json

from multiprocessing import Pool

### eliteprospects url destination
base_url = 'https://www.eliteprospects.com'
### default leagues

### table / database configurations
tables = {
    'team_standing' : {'csv' : 'team_stats',
                       'postgres' : 'team_stats'},
    'skaters' : {'csv' : 'skater_stats',
                 'postgres' : 'skater_stats'},
    'goalies' : {'csv' : 'goalie_stats',
                 'postgres' : 'goalie_stats'},
    'player_info' : {'csv' : 'player_info',
                     'postgres' : 'player_info'},
         }

def get_unique_players(player_stats, goalie_stats):
    '''This function takes skater and goalie stats and returns list of unique
    dataframe of playerids and player shortnames.
    '''
    player_cols = ['playerid', 'shortname']

    players = pd.concat([player_stats[player_cols].drop_duplicates(player_cols),
                         goalie_stats[player_cols].drop_duplicates(player_cols)])

    return players

def get_draft_eligibility(df):
    '''Return the first year a player is NHL draft eligible'''

    df.set_index('playerid', inplace=True)

    # create a series filled with valueus of Birth year + 18 on September 15th
    draft_days = pd.to_datetime(dict(year=df.date_of_birth.dt.year.values + 18,
                        month=np.full((len(df), ), 9),
                        day=np.full((len(df), ), 15)))

    draft_days.index = df.index

    # check if player will be 18 years old by September 15th of draft year
    df['draft_year_eligible'] = np.where((draft_days - df['date_of_birth']) / np.timedelta64(1, 'Y') >= 18,
                                             df.date_of_birth.dt.year.values + 18,
                                             df.date_of_birth.dt.year.values + 19)

    return df.reset_index()

def get_current_year(date):
    '''Return hockey season label based on what the current date is'''
    if date.month >= 1 and date.month <= 9:
        return f'{date.year - 1}-{date.year}'

    else:
        return f'{date.year}-{date.year + 1}'

def load_db_credentials(prod=False):
    '''Load database credentials from bash_profile'''

    if prod:
        user = os.environ['db_user_prod']
        password = os.environ['db_pass_prod']
        server = os.environ['db_host_prod']
        database = os.environ['db_name_prod']
        port = os.environ['db_port_prod']

    else:
        user = os.environ['db_user']
        password = os.environ['db_pass']
        server = os.environ['db_host']
        database = os.environ['db_name']
        port = os.environ['db_port']

    return user, password, server, database, port

def tidy_player_info(mydict, delete_keys):
    '''Remove keys from dictionary item that certain keys that do not want to be written to database'''

    keys_to_remove = set(delete_keys).intersection(set(mydict.keys()))
    for key in keys_to_remove:
        del mydict[key]

    return mydict

def clean_player_details(data):
    '''
    Cleans data format for player details data.
    '''

    player_info = {}

    for key, value in data.items():

        if key == 'Date of Birth':
            # get birthday in better format
            value = get_birthday(value)
            player_info['_'.join(key.lower().split(' '))] = value

        elif key == 'Height':
            value = get_height(value)
            player_info['_'.join(key.lower().split(' '))] = value

        elif key == 'Weight':
            value = get_weight(value)
            player_info['_'.join(key.lower().split(' '))] = value

        elif key == 'NHL Rights':
            player_info = {**player_info, **get_team_rights(value)}

        elif key == 'Drafted':
            player_info['draft_year'] = get_draft_year(value)
            player_info['draft_round'] = get_draft_round(value)
            player_info['draft_pick'] = get_draft_pick(value)
            player_info['draft_team'] = get_draft_team(value)

        else:
            player_info['_'.join(key.lower().split(' '))] = value

    return player_info

def get_draft_year(string):
    '''Using regular expression to return year a player was drafted from table row'''

    try:
        return re.split('[ ]', string)[0]
    except:
        return np.nan

def get_draft_round(string):
    '''Using regular expression to return round a player was drafted from table row'''

    try:
        return re.split('[ ]', string)[2]
    except:
        return np.nan

def get_draft_pick(string):
    '''Using regular expression to return the pick position a player was drafted from table row'''

    try:
        return re.split('[ ]', string)[3].strip('#')
    except:
        return np.nan

def get_draft_team(string):
    '''Using regular expression to return the team a player was drafted from table row'''

    try:
        return re.split('by', string)[1].strip()
    except:
        return np.nan

def get_team_rights(string):
    '''Using regular expression to return the current player rights from table row'''

    try:
        team, signed = string.split(' / ')
        return dict(rights=team, under_contract=True if signed == 'Signed' else False)
    except:
        return np.nan

def get_height(string):
    '''Using regular expression to return height in centimeter from table row'''

    try:
        return int(re.split('[/]', string)[0].replace('cm', '').strip())
    except:
        return np.nan

def get_weight(string):
    '''Using regular expression to return weight in kilograms from table row'''

    try:
        return int(re.split('[/]', string)[0].replace('kg', '').strip())
    except:
        return np.nan

def get_birthday(string):
    '''Using regular expression to extract player birthday from player url'''

    try:
        return datetime.datetime.strptime(string, "%b %d, %Y").strftime("%Y-%m-%d")
    except:
        return ''

def get_teamids(links):
    '''Using regular expression to return teamid from team url'''

    try:
        return [l.split('/')[4] for l in links]
    except:
        return ''

def get_shorthands(links):
    '''Using regular expression to return shorthands from url'''

    try:
        return [l.split('/')[5] for l in links]
    except:
        return ''

def get_playerids(links):
    '''Using regular expression to return playerid from player url'''

    try:
        return [l.split('/')[4] for l in links]
    except:
        return ''

def get_position(s):
    '''Using regular expression to return playerid from player url'''

    try:
        return re.search('\(([^)]+)', s).group(1).split('/')
    except:
        return ''

def clean_player_name(name):
    '''Using regular expression to return player name from table row'''

    return name[:np.negative(len(re.search('\(([^)]+)', name).group(1)) + 2)].strip()

def get_basic_player_info(soup):
    '''This function finds player details div and loops over the line items
    and creates a key/value dictionary containing player basic information'''

    player_details = soup.find('section', {'id' : 'player-facts'})

    main_details = player_details.find_all('li')
    ### clunky but it works
    player_info = {details.span.text.strip() : ','.join([a.text.replace('\n','').replace('\n','').strip().strip() \
                                                         for a in details.find_all('a')]) \
                   if details.a
                   else details.text[len(details.span.text):] \
                   for details in main_details}

    player_info = clean_player_details(player_info)

    return player_info

def get_add_player_info(soup, player_info):
    '''This function finds player details unlisted div and loops over the line items
    and creates a key/value dictionary containing player additional information. Returns
    a merged dictionary of basic player info and additional player info.'''

    # additional info in unlisted list
    add_info = soup.find('section', class_='plyr_details').find_all('ul', class_ = 'list-unstyled')

    # interate over div to get draft information and signed status if exists
    draft_info = {}
    for list_ in add_info:
        for info in list_.find_all('li'):
            if info.find('div', class_='col-xs-3 fac-lbl-light'):
                key = info.find('div', class_='col-xs-3 fac-lbl-light').text.replace('\n','').strip()
                value = info.find('div', class_='col-xs-9 fac-lbl-dark').text.replace('\n','').strip()

                draft_info['_'.join(key.lower().split(' '))] = value

            if 'nhl_rights' in draft_info:
                draft_info = {**draft_info, **get_team_rights(draft_info['nhl_rights'])}
            if 'drafted' in draft_info:
                draft_info = {**draft_info, **get_draft_year(draft_info['drafted'])}
                draft_info = {**draft_info, **get_draft_round(draft_info['drafted'])}
                draft_info = {**draft_info, **get_draft_pick(draft_info['drafted'])}
                draft_info = {**draft_info, **get_draft_team(draft_info['drafted'])}

    return {**player_info, **draft_info}

def get_team_league_stats(league, year):
    '''This function takes a league name and year and retrieves standings and team stats.
    Returns standings, teamidis and team shorthands to retrieve player roster information.'''

    # Define the URL template with placeholders for slug and season
    url_template = (
        "https://gql.eliteprospects.com/?operationName=LeagueStandingsAndSeasons"
        "&variables=%7B%22slug%22%3A%22{slug}%22%2C%22season%22%3A%22{season}%22%2C%22sort%22%3A%22group%2Cposition%22%7D"
        "&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%224f1e610c1de32cb243a476115c040505521fa2038dd3d2a7fd34e0ecd0d0c800%22%7D%7D"
    )

    # Format the URL with the given slug and season
    url = url_template.format(slug=urllib.parse.quote(league.lower()), season=urllib.parse.quote(year))

    # Define the necessary headers
    headers = {
        "Content-Type": "application/json",
        "x-apollo-operation-name": "LeagueStandingsAndSeasons",
        "apollo-require-preflight": "true"
    }

    # Make the request with the headers
    response = requests.get(url, headers=headers)

    data = json.loads(response.text)

    columns = ['gp', 'w', 't', 'l', 'otw',
               'otl', 'gf', 'ga', 'gd', 'tp']

    standings = pd.DataFrame([d['stats'] for d in data['data']['leagueStandings']]).rename(columns={'PTS' : 'tp'})
    standings.columns = [col.lower() for col in standings.columns]

    if standings.empty:
        return pd.DataFrame(), [], []

    teaminfo = pd.DataFrame([d['team'] for d in data['data']['leagueStandings']])

    teams = pd.DataFrame([d['teamName'] for d in data['data']['leagueStandings']])\
        .rename(columns={0:'team'})

    teams['teamid'] = teaminfo.eliteprospectsUrlPath.apply(lambda x : x.split('/')[2])
    teams['season'] = year
    teams['shortname'] = teaminfo.eliteprospectsUrlPath.apply(lambda x : x.split('/')[3])
    teams['league'] = league
    teams['url'] = base_url + '/team/' + teaminfo.eliteprospectsUrlPath + '/' + year

    team_standings = teams.merge(standings[columns], left_index=True, right_index=True)

    return team_standings, [(id_, name) for id_, name in zip(team_standings.teamid, team_standings.team)]

def get_skater_stats(year, teamid, team, league):
    '''This function takes a teamid, team name and year and retrieves team skater stats.
    Returns skater scoring data after calculating basic metrics.'''


    stat_cols = ['GP', 'G', 'A', 'PTS', 'PIM', 'PM']
    player_cols = ['player', 'position', 'playerid', 'url', 'shortname']

    # Define the URL template with placeholders for parameters
    url_template = (
        "https://gql.eliteprospects.com/?operationName=SkaterStats"
        "&variables=%7B%22team%22%3A%22{team}%22%2C%22season%22%3A%22{season}%22%7D"
        "&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%22730d3c8fa9edbcfb2a37f86303688d7a13595d9a0fa11f6167bbef789eaf9e65%22%7D%7D"
    )


    # Format the URL with the given parameters
    url = url_template.format(
        team=urllib.parse.quote(teamid),
        season=urllib.parse.quote(year)
    )

    # Define the necessary headers
    headers = {
        "Content-Type": "application/json",
        "x-apollo-operation-name": "SkaterStats",
        "apollo-require-preflight": "true"
    }

    # Make the request with the headers
    response = requests.get(url, headers=headers)
    data = json.loads(response.text)

    stages = [s for s in data['data']['playerStats']['edges'][0] if 'Stats' in s]

    players = pd.DataFrame([d['player'] for d in data['data']['playerStats']['edges']])\
    .rename(columns={'name' : 'player', 'detailedPosition' : 'position'})
    players['playerid'] = players.eliteprospectsUrlPath.apply(lambda x : x.split('/')[2])
    players['url'] = base_url + '/player/' + players.eliteprospectsUrlPath
    players['shortname'] = players.eliteprospectsUrlPath.apply(lambda x : x.split('/')[3])

    player_stats = []
    for stage in stages:
        stats = pd.DataFrame(
            [d[stage] if d[stage] else { col: 0 for col in stat_cols} \
             for d in data['data']['playerStats']['edges']])\
                                  .assign(season_stage=stage)\
                                  .rename(columns={'PTS' : 'TP'})\
                                  .drop(columns=['__typename'], errors='ignore')

        stats.columns = [col.lower() for col in stats.columns]
        # prevent zero divide error and remove all players with 0 games played
        stats = stats[stats['gp'] > 0]

        # calculate metrics for players
        player_stats.append(calculate_player_metrics(stats))

    player_stats = pd.concat(player_stats)

    player_stats['year'] = year
    player_stats['team'] = team
    player_stats['teamid'] = teamid
    player_stats['league'] = league

    player_stats = players[player_cols].merge(player_stats, left_index=True, right_index=True)

    return player_stats

def get_goalie_stats(year, teamid, team, league):

    '''This function takes a teamid, team name and year and retrieves team goalie stats.
    Returns goalie scoring data.'''

    player_cols = ['player', 'playerid', 'url', 'shortname']
    stat_cols = ['GP', 'GAA', 'SVP']

    # Define the URL template with placeholders for parameters
    url_template = (
        "https://gql.eliteprospects.com/?operationName=GoaltenderStats"
        "&variables=%7B%22team%22%3A%22{team}%22%2C%22season%22%3A%22{season}%22%7D"
        "&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%228ee15f99f463d0255abff7be71c7342f1705c19cbc1aa6ec5e4c4ded9b4ae146%22%7D%7D"
    )

    # Format the URL with the given parameters
    url = url_template.format(
        team=urllib.parse.quote(teamid),
        season=urllib.parse.quote(year)
    )

    # Define the necessary headers
    headers = {
        "Content-Type": "application/json",
        "x-apollo-operation-name": "GoaltenderStats",
        "apollo-require-preflight": "true"
    }

    # Make the request with the headers
    response = requests.get(url, headers=headers)

    data = json.loads(response.text)
    stages = [s for s in data['data']['playerStats']['edges'][0] if 'Stats' in s]

    players = pd.DataFrame([d['player'] for d in data['data']['playerStats']['edges']])\
    .rename(columns={'name' : 'player'})
    players['playerid'] = players.eliteprospectsUrlPath.apply(lambda x : x.split('/')[2])
    players['url'] = base_url + '/player/' + players.eliteprospectsUrlPath
    players['shortname'] = players.eliteprospectsUrlPath.apply(lambda x : x.split('/')[3])

    goalie_stats = []
    for stage in stages:
        stats = pd.DataFrame(
            [d[stage] if d[stage] else { col: 0 for col in stat_cols} \
             for d in data['data']['playerStats']['edges']])\
                                  .assign(season_stage=stage)\
                                  .rename(columns={'PTS' : 'TP'})\
                                  .drop(columns=['__typename'], errors='ignore')

        stats.columns = [col.lower() for col in stats.columns]
        # prevent zero divide error and remove all players with 0 games played
        stats = stats[stats['gp'] > 0]
        goalie_stats.append(stats)

    goalie_stats = pd.concat(goalie_stats)

    goalie_stats = players[player_cols].merge(goalie_stats[stats.columns], left_index=True, right_index=True)

    goalie_stats['year'] = year
    goalie_stats['team'] = team
    goalie_stats['teamid'] = teamid
    goalie_stats['league'] = league

    return goalie_stats

def get_player_stats(year, teamid, teamshort, league):
    '''This function takes a teamid, team name and year and retrieves team goalie and skater stats.
    Returns goalie / skater scoring data as a wrapper around individual position functions.'''

    # contruct url
    url = f'{base_url}/team/{teamid}/{teamshort}/{year}?tab=stats#players'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, features="lxml")

    try:
        # get stats from goalies and skaters
        return get_skater_stats(year, teamid, teamshort, league), get_goalie_stats(year, teamid, teamshort, league)

    except Exception as e:
        print(f'\n{year} {teamshort} does not have have proper team stats \n')
        print('Error: \n', e)
        return pd.DataFrame(columns=['player', 'gp', 'g',
                                     'a', 'tp', 'pim', 'pm']), pd.DataFrame(columns=['player', 'gp',
                                                                                     'gaa', 'svp'])

def calculate_player_metrics(df):
    '''Takes a dataframe containing player stats for a team and calcuates metrics intra-team.
    Returns points per game, assists per game, goals per game, percent of team points, etc.
    '''
    try:
        # adding metrics
        df = df.assign(
            gpg=(df.g.astype(int) / df.gp.astype(int)).round(3),
            apg=(df.a.astype(int) / df.gp.astype(int)).round(3),
            ppg=(df.tp.astype(int) / df.gp.astype(int)).round(3),
            perc_team_g=((df.g.astype(int) / df.gp) / (df.g.astype(int).sum() / df.gp.max())).round(3),
            perc_team_a=((df.a.astype(int) / df.gp) / (df.a.astype(int).sum() / df.gp.max())).round(3),
            perc_team_tp=((df.tp.astype(int) / df.gp) / (df.tp.astype(int).sum() / df.gp.max())).round(3))

        return df

    except:

        return df

def scrape_league_season_stats(league, year):
    '''This function is a wrapper takes a league name and year and retrieve team league
    standings data, skater scoring statistics, traditional goalie statistics.
    '''

    print(f'\n--- Getting League Team Stats for {league} {year} --- \n')
    league_player_stats = []
    league_goalie_stats = []
    # get league standings for teams
    team_standings, team_info = get_team_league_stats(league, year)
    if team_standings.empty:
        team_info = get_league_teams(league, year)

    # loop over teams to construct player stat tables
    for teamid, team in team_info:
        try:
            print(f'--- Getting Team Player Stats for {team} {teamid} ---')
            player_stats = get_skater_stats(year, teamid, team, league)
            goalie_stats = get_goalie_stats(year, teamid, team, league)

            league_player_stats.append(player_stats)
            league_goalie_stats.append(goalie_stats)
            # space url calls by 2 second each time
            time.sleep(2)
        except Exception as e:
            print(f'\n--- Failed to load {team} {teamid} ---')
            print(e)
            continue

    player_stats = pd.concat(league_player_stats, sort=False)
    goalie_stats = pd.concat(league_goalie_stats, sort=False)

    player_stats = player_stats.assign(league=league)
    goalie_stats = goalie_stats.assign(league=league)

    return team_standings, player_stats, goalie_stats

def get_league_teams(league, year):

    ''' loop over gql api instead of divs '''

    url_template = (
    "https://gql.eliteprospects.com/?operationName=LeagueTeamComparison"
    "&variables=%7B%22slug%22%3A%22{slug}%22%2C%22season%22%3A%22{season}%22%2C%22sort%22%3A%22team.name%22%7D"
    "&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%227b72c1dc0a2e7e390c7887b9f48369c7749dc5aa4be1168e95f384e87feb21b9%22%7D%7D"
)

    # Format the URL with the given slug and season
    url = url_template.format(slug=urllib.parse.quote(league.lower()), season=urllib.parse.quote(year))

    # Define the necessary headers
    headers = {
        "Content-Type": "application/json",
        "x-apollo-operation-name": "LeagueStandingsAndSeasons",
        "apollo-require-preflight": "true"
    }

    # Make the request with the headers
    response = requests.get(url, headers=headers)

    data = json.loads(response.text)

    return [(d['team']['id'], d['team']['name']) for d in data['data']['leagueTeamComparison']]

def _scrape_league_season_stats(league, year):
    '''This function is a wrapper takes a league name and year without league
    standings data, and returns skater scoring statistics, traditional goalie statistics.
    '''

    print(f'\n--- Getting League Team Stats for {league} {year} --- \n')
    league_player_stats = []
    league_goalie_stats = []
    # get league standings for teams
    team_info = get_league_teams(league, year)

    # loop over teams to construct player stat tables
    for teamid, teamshort in team_info:
        try:
            print(f'--- Getting Team Player Stats for {teamshort} {teamid} ---')
            player_stats, goalie_stats = get_player_stats(year, teamid, teamshort, league)

            league_player_stats.append(player_stats)
            league_goalie_stats.append(goalie_stats)
            # space url calls by 1 second each time
            time.sleep(2)
        except Exception as e:
            print(f'\n--- Failed to load {teamshort} {teamid} ---')
            print(e)
            continue

    player_stats = pd.concat(league_player_stats, sort=False)
    goalie_stats = pd.concat(league_goalie_stats, sort=False)

    player_stats = player_stats.assign(league=league)
    goalie_stats = goalie_stats.assign(league=league)

    return player_stats, goalie_stats

def get_player_info(playerid, shortname):
    ''' This function takes a playerid and player shortname and retrieve all scrapable
    player information from their player page.
    '''


    try:
        print(f'--- Retrieving player info for: {shortname}')

        url = f'{base_url}/player/{playerid}/{shortname}'
        r = requests.get(url)
        soup = BeautifulSoup(r.text, features="lxml")

        delete_keys = ['age', 'youth_team', 'agency', 'highlights',
                       'drafted', 'cap_hit', 'nhl_rights', 'player_type']

        player_info = get_basic_player_info(soup)
        player_info = tidy_player_info(player_info, delete_keys)

        player_info = pd.DataFrame([player_info])
        player_info['date_of_birth'] = pd.to_datetime(player_info['date_of_birth'])
        player_info['playerid'] = playerid
        player_info['shortname'] = shortname

        # space url calls by 1 second each time
        time.sleep(0.5)

        return player_info

    except Exception as e:
        print(f'--- failed to get player info for: {shortname} \n {e}')

class Scraper(object):

    def __init__(self,
        leagues= [
            'CCHL2','NHL','NLA','USHL','BCHL', 'SJHL', 'AHL','SuperElit', 'WHL',
            'CCHL','U20 SM-liiga', 'OHL','MHL','SHL','NCAA','Liiga', 'DEL', 'Slovakia', ### 'Jr. A SM-liiga' renamed to 'U20 SM-liiga' by EP
            'HockeyAllsvenskan','QMJHL','AJHL','OJHL','KHL','VHL','Czech','Czech2',
            'USHS-PREP', 'ECHL', 'Mestis', 'NTDP'],
        start_year = 1985,
        end_year = 2020,
        prod_db = False
        ):

        self.leagues = leagues
        self.start_year = start_year
        self.end_year = end_year
        self.seasons = [f'{s}-{s + 1}' for s in range(self.start_year, self.end_year + 1)]
        self.failed_league_seasons = []
        self.prod = prod_db

        # self.engine = engine

    def get_playerid_delta(self, players):

        # get db credentials
        user, password, server, database, port = load_db_credentials(self.prod)
        # create a connection to the database
        engine = create_engine(f'postgresql://{user}:{password}@{server}:{port}/{database}')

        player_ids = pd.read_sql('''
        select
            distinct playerid
        from
            public.player_info
            ''', self.engine)

        delta_players = players[~players.playerid.astype(int).isin(player_ids.playerid)]

        return delta_players

    def output_to_db(self, df, name):
        '''Writes a dataframe to database using the table metadata outlined at script instantiation'''

        # get db credentials
        user, password, server, database, port = load_db_credentials(self.prod)
        # create a connection to the database
        engine = create_engine(f'postgresql://{user}:{password}@{server}:{port}/{database}')
        # add a load date field
        df = df.assign(load_date = datetime.datetime.now())
        # write the values to the database
        df.to_sql(tables[name]['postgres'], engine, if_exists='append', index = False )

    def output_to_csv(self, df, name):
        '''Writes a dataframe to csv file using the table metadata outlined at script instantiation'''

        date = datetime.date.today().strftime('%Y-%m-%d')

        if not os.path.exists('data'):
            os.makedirs('data')

        table = tables[name]['csv']
        # write the values to the database
        df.to_csv(f'data/{table}_{date}.csv', index=False)

    def full_data_load(self, collect_player_info=False, output='csv'):

        '''This function is the main wrapper for a full load of elite prospects data. Leagues and Years
        are initialized, then looped over to retrieve team league standings, skater/goalie statistics and
        player information. Will always return a CSV output of the 4 main files and also has functionality
        to update tables in a postgres database.
        '''

        # get date time of when script starts
        start = time.time()

        # initialize lists to hold data frames
        team_stats = []
        player_stats = []
        goalie_stats = []
        player_info = []

        for league in self.leagues:
            failed_seasons = []

            for year in self.seasons:
                try:
                    # get team, skaters & goalies stats from league page
                    teams, players, goalies = scrape_league_season_stats(league, year)
                    team_stats.append(teams)
                    player_stats.append(players)
                    goalie_stats.append(goalies)

                    # write data to database after each league season loaded
                    if output == 'postgres':
                        self.output_to_db(teams, 'team_standing')
                        self.output_to_db(players, 'skaters')
                        self.output_to_db(goalies, 'goalies')

                except Exception as e:
                    print(e)

                    failed_seasons.append(year)
                    continue

            self.failed_league_seasons.append(
                {
                    'league' :league,
                    'seasons' : failed_seasons
                    }
                    )

        team_stats = pd.concat(team_stats, sort=False)
        player_stats = pd.concat(player_stats, sort=False)
        goalie_stats = pd.concat(goalie_stats, sort=False)

        # output to csv always
        self.output_to_csv(team_stats, 'team_standing')
        self.output_to_csv(player_stats, 'skaters')
        self.output_to_csv(goalie_stats, 'goalies')

        if collect_player_info:

            # get player info for skaters and goalies
            players = get_unique_players(player_stats, goalie_stats)

            for playerid, shortname in zip(players.playerid, players.shortname):
                try:
                    player = get_player_info(playerid, shortname)
                    player_info.append(player)
                except:
                    print(f'--- {shortname} bad player data ---')
                    continue

            player_info = pd.concat(player_info, sort=False)
            # gets draft eligibility for all players
            player_info = get_draft_eligibility(player_info)
            self.output_to_csv(player_info, 'player_info')

            if output == 'postgres':
                self.output_to_db(player_info, 'player_info')

        print('Runtime : {} mins'.format(round((time.time() - start) / 60 ,2)))
        print('Re-run the following league seasons: ', self.failed_league_seasons)

    def delta_data_load(self, failed_league_seasons=[], output='csv'):

        '''This function is the main wrapper for a delta load of elite prospects data. Leagues and Years
        are passed to the function, then looped over to retrieve team league standings, skater/goalie statistics and
        player information. Will always return a CSV output of the 4 main files and also has functionality
        to update tables in a postgres database.
        '''

        # get date time of when script starts
        start = time.time()

        # initialize lists to hold data frames
        team_stats = []
        player_stats = []
        goalie_stats = []
        player_info = []

        for league_seasons in failed_league_seasons:
            for year in league_seasons['seasons']:
                try:
                    # get team, skaters & goalies stats from league page
                    teams, players, goalies = scrape_league_season_stats(league_seasons['league'], year)
                    team_stats.append(teams)
                    player_stats.append(players)
                    goalie_stats.append(goalies)

                    # write data to database after each league season loaded
                    if output == 'postgres':
                        self.output_to_db(teams, 'team_standing')
                        self.output_to_db(players, 'skaters')
                        self.output_to_db(goalies, 'goalies')

                except Exception as e:
                    print(e)
                    try:
                        # some leagues do not have standings
                        players, goalies = _scrape_league_season_stats(league_seasons['league'], year)
                        player_stats.append(players)
                        goalie_stats.append(goalies)

                        # write data to database after each league season loaded
                        if output == 'postgres':
                            self.output_to_db(teams, 'team_standing')
                            self.output_to_db(players, 'skaters')
                            self.output_to_db(goalies, 'goalies')
                    except Exception as e:
                        print(f"\n---{league_seasons['league']} {year} not found---\n")
                        print(e)
                        continue

        team_stats = pd.concat(team_stats, sort=False)
        player_stats = pd.concat(player_stats, sort=False)
        goalie_stats = pd.concat(goalie_stats, sort=False)

        ### retrieve player information by finding the unique / delta players names
        players = get_unique_players(player_stats, goalie_stats)

        delta_players = self.get_playerid_delta(players)

        with Pool(processes=8) as pool:
            pool.starmap(get_player_info, zip(delta_players.playerid, delta_players.shortname))

        player_info = pd.concat(player_info, sort=False)

        player_info = get_draft_eligibility(player_info)
        self.output_to_csv(player_info, 'player_info')

        if output == 'postgres':
            self.output_to_db(player_info, 'player_info')

        print('Runtime : {} mins'.format(round((time.time() - start) / 60 ,2)))
