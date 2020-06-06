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

# leagues tested
leagues = ['NHL', 'NLA', 'USHL', 'BCHL', 'SJHL', 'AHL', 'SuperElit', 'WHL',
            'CCHL', 'Jr. A SM-liiga', 'OHL', 'MHL', 'SHL', 'NCAA', 'Liiga',
            'Allsvenskan', 'QMJHL', 'AJHL', 'OJHL', 'KHL', 'VHL', 'Czech', 'Czech2']

class Scraper(object):

    def __init__(self,
        leagues=leagues,
        start_year = 2005,
        end_year = 2019):

        self.leagues = leagues,
        self.start_year = start_year,
        self.end_year = end_year
        self.seasons = [f'{s}-{s + 1}' for s in range(self.start_year, self.end_year + 1)]
        self.base_url = 'https://www.eliteprospects.com'
