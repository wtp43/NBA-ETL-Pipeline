import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) + '/db/')
import sql_func as sif
import db_func
import scrape_historical_data as shd
from dotenv import load_dotenv
from pathlib import Path

import psycopg2
from psycopg2 import Error
import logging
from multiprocessing import Pool, cpu_count, Lock
from multiprocessing.pool import ThreadPool
from timeit import default_timer as timer
import traceback
from datetime import date, datetime
import itertools
import csv
from pandas.core.frame import DataFrame
import pandas as pd
import ssl
import bbref_scraper as b

#from db_func import threaded_postgreSQL_pool

# conn = db_func.get_conn()
# cur = conn.cursor()
# conn.autocommit = True
# db_func.truncate_imports(cur)
# file_path = 'csv/players/b/batises01.csv'
# sif.insert_to_imports(file_path)

# conn.close()

# with open('csv/player_list.csv', newline='', mode='w+') as f:
# 	f.truncate()
# teams = b.get_teams(('2021',))
# print(teams)

#sif.insert_to_imports('sdf')
html = '/home/wt/Projects/NBA-Stats-Database/scrape/bs4_html/players/v/vildolu01.html' 
bbref_endpoint = '/players/v/vildolu01.html'
player_name = 'Luca Vildoza'
shd.player_data_to_csv(html, bbref_endpoint, player_name)