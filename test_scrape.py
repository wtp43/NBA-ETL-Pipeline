import scrape_historical_data as shd
import sql_insert_funcs as sif
from datetime import date, datetime
import db_func
import db_config
import scrape_historical_data as shd
import psycopg2
from psycopg2 import pool
from psycopg2 import OperationalError, errorcodes, errors, Error
import sys
import os
import logging
from multiprocessing import Pool, cpu_count, Lock
import time
from timeit import default_timer as timer
import traceback
from datetime import date, datetime
import itertools
import csv
import scrape_bb_ref_driver as sbrd
import traceback
from bs4 import BeautifulSoup
import requests
import pandas as pd

DEBUG = False

def init_child(lock_):
	global lock
	global conn
	conn = db_func.get_conn()
	lock = lock_


def main():
	team = "DAL"
	#date = "201910230"
	match_id = '1'
	#shd.save_boxscore_html(team, date)
	#html_path = "bs4_html/boxscores/201910230DAL.html"
	#shd.save_player_data(html_path)
	#shd.save_match_html('2021')
	#shd.save_match_data('bs4_html/match_list/2021.html')
	#shd.save_all_player_performances('2021')
	# conn=db_func.get_conn()
	# cur = conn.cursor()
	# res = imp.get_all_matches(cur)
	#print(res)



	if not os.path.isdir("logs"):
		os.makedirs("logs")
	logging.basicConfig(level=logging.DEBUG, 
						format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
     					datefmt='%Y-%m-%d %H:%M:%S',
						handlers=[
							logging.FileHandler('logs/'+date.today().strftime("%Y-%m-%d") + '.log'),
							logging.StreamHandler()])	

	try:
		conn = db_func.get_conn()
		cur = conn.cursor()
		seasons = ['2021','2020']
		html = 'bs4_html/players/l/luety01.html'
		bbref_endpoint='/players/l/luety01.html'
		player_name = 'ty'
		#err = shd.player_data_to_csv(html, bbref_endpoint, player_name)
		#print(err)
		directory = "csv/" + '2018'
		if not os.path.isdir(directory):
			os.makedirs(directory)
		file_path = 'csv/2018/match_list.csv'
		team_abbr = shd.get_team_abbr()
		regular_game = True
		playoff_index = 0

		url = 'https://www.basketball-reference.com/leagues/NBA_2018_games-april.html'
		html = 'bs4_html/leagues/NBA_2018_games-april.html'
		shd.save_html(url, html)
		with open(html, 'r', encoding="utf8") as f:
			contents = f.read()
			soup = BeautifulSoup(contents, 'lxml')

		
		table = soup.find_all('table', attrs={'id': 'schedule'})
		df = pd.read_html(str(table), flavor='bs4', header=[0])[0]
		playoff_index = df[df['Date']=='Playoffs'].index
		print(playoff_index)
		df['playoff_game'] = 0
		if len(playoff_index):

			df = df[df.index != playoff_index[0]]
			df.loc[playoff_index[0]:, 'playoff_game'] = 1

		df.drop(columns=df.columns[[1,6,7,8,9]], inplace=True)
		df.rename(columns={'Date':'date', 'Visitor/Neutral': 'away_id', 
			'Home/Neutral': 'home_id', 'PTS': 'away_pts', 'PTS.1': 'home_pts'}, inplace=True)

		df['date'] = pd.to_datetime(df.date)
		df = df[df['away_pts'].notna()]
		df['date'] = df['date'].dt.strftime('%Y%m%d')
		df['away_id'] = df['away_id'].apply(lambda x: team_abbr[x])
		df['home_id'] = df['home_id'].apply(lambda x: team_abbr[x])

		#The page that contains the start of playoff table
		#only needs rows after it modified
		#While all pages after only contain playoff games
		df.to_csv(file_path, mode='a',index=False, header=False)
		print(df)
		print(playoff_index)
	
	except Exception as err:
		logging.exception(traceback.print_exception(*sys.exc_info()))
		sys.exit()
	finally:
		if (conn):
			conn.close()
			print("PostgreSQL connection is closed")

def test_process_players(cur, season):
	sbrd.process_players(cur, ['2021'])
	sbrd.get_player_endpoints()

def populate_team_table(cur):
	team_list_path = 'db_src/NBA_Teams.csv'
	sif.insert_teams(team_list_path,cur)

def test_get_teams():
	for i in range(2010, 2021):
		teams = sbrd.get_teams(i)
		#assert len(teams) == 30
		print(teams)
		print(i, len(teams))


if __name__ == '__main__':
	main()