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
        return int(re.split('[/]', string)[1].strip('cm').strip())
    except:
        return np.nan

def get_weight(string):
    '''Using regular expression to return weight in kilograms from table row'''

    try:
        return int(re.split('[/]', string)[1].strip('kg').strip())
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

    player_details = soup.find('div', class_ = 'ep-list')
    main_details = player_details.find_all('div', class_='col-xs-12')
    player_info = {k.text.strip() : v.text.replace('\n','').strip().strip() for k,v in zip(main_details[0::2], main_details[1::2])}

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
    # find current season
    current_season = get_current_year(datetime.date.today())

    # contruct url
    league_path = '-'.join(league.lower().split(' '))
    url = f'{base_url}/league/{league_path}/{year}'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, features="lxml")

    # get standings
    standings = soup.find('table', {'class': 'table standings table-sortable'})

    team_links = [] # hyperlinks
    columns = ['team', 'gp', 'w', 't', 'l', 'otw',
               'otl', 'gf', 'ga', 'gd', 'tp'] # columns to return
    # get all rows for table
    table_rows = standings.find_all('tr')
    data = []
    for tr in table_rows:
        rows = []
        td = tr.find_all('td')
        # check if there are post season / playout / relegation stats
        if tr.attrs:
            if tr['class'][0] == 'title':
                header = tr.text.replace('\n','').strip()
                if header in ['Playout', 'Relegation']:
                    break

        for t in td:
            try:
                if t['class'][0] in columns:
                    # if there is a link find the hyperlinks
                    if t.find('a'):
                        team_links.append(t.find('a').get('href'))
                    rows.append(t.text.replace('\n',''))
            except:
                # only add data that are contained in columns or have a class
                continue

        data.append(rows)

    team_standings = pd.DataFrame(data, columns=columns).dropna().replace(
        {'-': np.nan}).replace({'': np.nan}).replace({' - ': np.nan}).fillna(0)

    team_standings = team_standings.apply(pd.to_numeric, errors="ignore")
    # get team ids from url
    teamids = get_teamids(team_links)
    # get lowercase/hyphenated team name from url
    teamshorts = get_shorthands(team_links)
    # check team urls for current season
    if len(team_links[0].split('/')) != 7:
        team_links = [l + f'/{current_season}' for l in team_links]

    # add as metadata keys to df
    team_standings['season'] = year
    team_standings['teamid'] = teamids
    team_standings['shortname'] = teamshorts
    team_standings['url'] = team_links
    team_standings['league'] = league

    # aggregate regular season stats
    team_standings = team_standings.groupby(
        ['team', 'teamid', 'season', 'shortname', 'league', 'url']).sum().reset_index()

    return team_standings, team_standings.teamid, team_standings.shortname

def get_skater_stats(soup, year, teamid, teamshort, league):
    '''This function takes a teamid, team name and year and retrieves team skater stats.
    Returns skater scoring data after calculating basic metrics.'''

    stats_table = soup.find('table',
                            {'class': 'table table-striped table-sortable skater-stats highlight-stats'})
    columns = ['season_stage', 'player', 'gp', 'g', 'a', 'tp', 'pim', 'pm']
    player_links = []

    # get all table rows from html table div minus title
    table_rows = stats_table.find_all('tr')[1:]
#     headers = set(stats_table.find_all('tr', class_ = ['title']))
#     table_rows -= headers

    data = []
    for tr in table_rows:
        # only retrieve regular season stats
        if tr.attrs:
            if 'class' in tr.attrs.keys():
                if tr['class'][0] == 'title':
                    header = tr.text.replace('\n','').strip()
                    if header != league and header[-1] != league:
                        season_stage = header
                        continue # don't want to add header to list of player stats
                    else:
                        season_stage = "Regular Season"
                        continue # don't want to add header to list of player stats

        rows = [season_stage]
        td = tr.find_all('td')
        for t in td:
            try:
                if t['class'][0] in columns:
                    # if there is a link find the hyperlinks
                    if t.find('a'):
                        player_links.append(t.find('a').get('href'))
                    rows.append(t.text.replace('\n','').strip())
            except:
                # only add data that are contained in columns or have a class
                continue

        data.append(rows)

    # create dataframe for data
    player_stats = pd.DataFrame(data, columns=columns).dropna() # drop empty values
    player_stats = player_stats[player_stats.player != ''] # empty string players entries
    player_stats = player_stats.replace(
        {'-': np.nan}).replace({'': np.nan}).fillna(0).apply(pd.to_numeric, errors="ignore")

    # clean up dataframe -- add position, playerid, team info
    player_stats['position'] = player_stats.player.apply(get_position)
    player_stats['player'] = player_stats.player.apply(clean_player_name)
    player_stats['team'] = teamshort[:3].upper()
    player_stats['teamid'] = teamid
    player_stats['playerid'] = get_playerids(player_links)
    player_stats['year'] = year
    player_stats['url'] = player_links
    player_stats['shortname'] = get_shorthands(player_links)
    # calculate metrics for players
    player_stats = player_stats.groupby(['season_stage']).apply(calculate_player_metrics)

    return player_stats.droplevel(0)

def get_goalie_stats(soup, year, teamid, teamshort, league):
    '''This function takes a teamid, team name and year and retrieves team goalie stats.
    Returns goalie scoring data.'''

    stats_table = soup.find('table',
                            {'class': 'table table-striped table-sortable goalie-stats highlight-stats'})
    columns = ['season_stage', 'player', 'gp', 'gaa', 'svp']

    goalie_links = []

    # get all table rows from html table div minus title
    table_rows = stats_table.find_all('tr')[1:]
#     headers = set(stats_table.find_all('tr', class_ = ['title']))
#     table_rows -= headers

    data = []
    for tr in table_rows:
        # only retrieve regular season stats
        if tr.attrs:
            if 'class' in tr.attrs.keys():
                if tr['class'][0] == 'title':
                    header = tr.text.replace('\n','').strip()
                    if header != league and header[-1] != league:
                        season_stage = header
                        continue # don't want to add header to list of player stats
                    else:
                        season_stage = "Regular Season"
                        continue # don't want to add header to list of player stats

        rows = [season_stage]
        td = tr.find_all('td')
        for t in td:
            try:
                if t['class'][0] in columns:
                    # if there is a link find the hyperlinks
                    if t.find('a'):
                        goalie_links.append(t.find('a').get('href'))
                    rows.append(t.text.replace('\n','').strip())
            except:
                # only add data that are contained in columns or have a class
                continue

        data.append(rows)

    # create dataframe for data
    goalie_stats = pd.DataFrame(data, columns=columns).dropna() # drop empty values
    goalie_stats = goalie_stats[goalie_stats.player != ''] # empty string players entries
    goalie_stats = goalie_stats.replace(
    {'-': np.nan}).replace({'': np.nan}).fillna(0).apply(pd.to_numeric, errors="ignore")

    goalie_stats['team'] = teamshort[:3].upper()
    goalie_stats['teamid'] = teamid
    goalie_stats['playerid'] = get_playerids(goalie_links)
    goalie_stats['year'] = year
    goalie_stats['url'] = goalie_links
    goalie_stats['shortname'] = get_shorthands(goalie_links)

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
        return get_skater_stats(soup, year, teamid, teamshort, league), get_goalie_stats(soup, year, teamid, teamshort, league)

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
    team_standings, teamids, teamshorts = get_team_league_stats(league, year)

    # loop over teams to construct player stat tables
    for teamid, teamshort in zip(teamids, teamshorts):
        try:
            print(f'--- Getting Team Player Stats for {teamshort} {teamid} ---')
            player_stats, goalie_stats = get_player_stats(year, teamid, teamshort, league)

            league_player_stats.append(player_stats)
            league_goalie_stats.append(goalie_stats)
            # space url calls by 2 second each time
            time.sleep(2)
        except Exception as e:
            print(f'\n--- Failed to load {teamshort} {teamid} ---')
            print(e)
            continue

    player_stats = pd.concat(league_player_stats, sort=False)
    goalie_stats = pd.concat(league_goalie_stats, sort=False)

    player_stats = player_stats.assign(league=league)
    goalie_stats = goalie_stats.assign(league=league)

    return team_standings, player_stats, goalie_stats

def get_league_teams(league, year):
    # if there are no standings, loop over team rosters: 'list-as-columns'
    url = f'https://www.eliteprospects.com/league/{league}/{year}'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, features="lxml")
    teams = soup.find('div', class_='list-as-columns')
    teams = [t.find('a') for t in teams.find_all('li')]

    return get_teamids([t.get('href') for t in teams]), get_shorthands([t.get('href') for t in teams])

def _scrape_league_season_stats(league, year):
    '''This function is a wrapper takes a league name and year without league
    standings data, and returns skater scoring statistics, traditional goalie statistics.
    '''

    print(f'\n--- Getting League Team Stats for {league} {year} --- \n')
    league_player_stats = []
    league_goalie_stats = []
    # get league standings for teams
    teamids, teamshorts = get_league_teams(league, year)

    # loop over teams to construct player stat tables
    for teamid, teamshort in zip(teamids, teamshorts):
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

    print(f'--- Retrieving player info for: {shortname}')

    url = f'{base_url}/player/{playerid}/{shortname}'
    r = requests.get(url)
    soup = BeautifulSoup(r.text, features="lxml")

    delete_keys = ['age', 'youth_team', 'agency', 'highlights',
                   'drafted', 'cap_hit', 'nhl_rights']

    player_info = get_basic_player_info(soup)
    player_info = tidy_player_info(player_info, delete_keys)

    player_info = pd.DataFrame([player_info])
    player_info['date_of_birth'] = pd.to_datetime(player_info['date_of_birth'])
    player_info['playerid'] = playerid
    player_info['shortname'] = shortname

    # space url calls by 1 second each time
    time.sleep(3)

    return player_info

class Scraper(object):

    def __init__(self,
        leagues= [
            'CCHL2','NHL','NLA','USHL','BCHL', 'SJHL', 'AHL','SuperElit', 'WHL',
            'CCHL','U20 SM-liiga', 'OHL','MHL','SHL','NCAA','Liiga', 'DEL', 'Slovakia', ### 'Jr. A SM-liiga' renamed to 'U20 SM-liiga' by EP
            'Allsvenskan','QMJHL','AJHL','OJHL','KHL','VHL','Czech','Czech2',
            'USHS-PREP', 'USDP',  'ECHL', 'Mestis'],
        start_year = 2005,
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
                    try:
                        # some leagues do not have standings
                        players, goalies = _scrape_league_season_stats(league, year)
                        player_stats.append(players)
                        goalie_stats.append(goalies)

                        # write data to database after each league season loaded
                        if output == 'postgres':
                            self.output_to_db(teams, 'team_standing')
                            self.output_to_db(players, 'skaters')
                            self.output_to_db(goalies, 'goalies')
                    except Exception as e:
                        print(f'\n---{league} {year} not found---\n')
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

        for playerid, shortname in zip(delta_players.playerid, delta_players.shortname):
            try:
                player = get_player_info(playerid, shortname)
                player_info.append(player)
            except:
                print(f'--- {shortname} bad player data ---')
                continue

        player_info = pd.concat(player_info, sort=False)

        player_info = get_draft_eligibility(player_info)
        self.output_to_csv(player_info, 'player_info')

        if output == 'postgres':
            self.output_to_db(player_info, 'player_info')

        print('Runtime : {} mins'.format(round((time.time() - start) / 60 ,2)))

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
