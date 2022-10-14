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
import time
from timeit import default_timer as timer
import traceback
from datetime import date, datetime
import itertools
import csv
from pandas.core.frame import DataFrame
import pandas as pd




DEBUG = False

def init_child(lock_):
	global lock
	lock = lock_
	

def get_all_seasons(cur):
	try:
		select_matches_query = \
		'''SELECT season
		FROM season;'''
		cur.execute(select_matches_query)
		return list(cur.fetchall())
	except Error as err:
		raise err


def mproc_save_player_endpoints(team,season):
	'''
    mproc_save_player_endpoints 

	Args: 
		:param team: endpoint of player on bbref
			ex: /players/b/bareajo01.html
		:param season: given in a tuple
			ex: ('2020',)
	
	:side effect: csv with match stats
    :return: None
    '''
	try:
		season = season[0]
		url = 'https://www.basketball-reference.com/teams/' + team + \
			'/' + season + '.html'
		html = os.path.join(os.getcwd(),"bs4_html/roster/" + season + \
			"/" +team+".html")
		with lock:
			time.sleep(1.5)
			shd.save_html(url, html)
		df = shd.get_endpoints_df(html)

		csv = 'csv/player_list.csv'
		with lock:
			df.to_csv(csv, mode='a+',index=False, header=False)

	except Exception as err:
		logging.error("mproc_save_player_endpoints error: {}, {}".format(team, season))
		raise err


def mproc_insert_players(bbref_endpoint, player_name):
	'''
    Wrapper function 
		1. Save html of bbref_player_endpoint
		2. Scrape relevant player info from html 
		3. Save to CSV
		4. Copy CSV to player import_table

	Args: 
		:param bbref_player_endpoint: html player endpoint on bbref
			ex: /players/b/brownst02.html
		:param player_name: previously scraped
	
	:side effect: csv with match stats
    :return: None
    '''
	try:
		url = "https://www.basketball-reference.com" + bbref_endpoint 
		html = os.path.join(os.getcwd(),"bs4_html" + bbref_endpoint)
		
		with lock:
			time.sleep(1.5)
			shd.save_html(url, html)
		# with lock:
		# 	logging.info('bbref_endpoint: {}, player_name: {} scraped'
		# 	.format(bbref_endpoint, player_name)) 
		
		err = shd.player_data_to_csv(html, bbref_endpoint, player_name)
		if err:
			with lock:
				logging.info('bbref_endpoint: {}, player_name: {} NO DATA FOUND'
			.format(bbref_endpoint, player_name))
			return 
				
		# with lock:
		# 	logging.info('bbref_endpoint: {}, player_name: {} saved to csv'
		# 	.format(bbref_endpoint, player_name)) 
		
		#Insert player data into imports
		file_path = 'csv' + bbref_endpoint[:-5] + '.csv'
		sif.insert_to_imports(file_path)
		# with lock:
		# 	logging.info('endpoint: {}, player: {} inserted into imports table'
		# 	.format(bbref_endpoint, player_name)) 

		
	except Exception as err:
		raise err

def mproc_insert_matches(season):
	'''
    Wrapper function 
		1. Save html of match list
		2. Scrape relevant matches from html 
		3. Save to CSV
		4. Copy CSV to match_import table

	Args: 
		:param season: season given in a tuple
			ex: (2020,)
	:side effect: csv with match stats
    :return: None
    '''
	try:
		season = season[0]
		url = "https://www.basketball-reference.com/leagues/NBA_" + season \
				+ "_games.html"
		html = os.path.join(os.getcwd(),"bs4_html/match_list/" \
			+ "/" +season+".html")

		with lock:
			time.sleep(1.5)
			shd.save_html(url, html)
		# with lock:
		# 	logging.info(season+" season html saved")
		
		shd.match_list_to_csv(html)
		# with lock:
		# 	logging.info(season+" season matches saved to csv")
		
		csv = os.path.join(os.getcwd(),"csv/match_lists/" + season \
			+ "_match_list.csv")
		sif.insert_to_imports(csv)

		# with lock:
		# 	logging.info(season +": season matches inserted into imports table")

	except Exception as err:
		raise err


def process_matches(cur, seasons):
	db_func.truncate_imports(cur)
	lock = Lock()
	pool_size = cpu_count()-1
	print(f'starting computations on {pool_size} cores')
	with Pool(pool_size, initializer=init_child,initargs=(lock,)) as pool:
		pool.map(mproc_insert_matches, seasons)
	sif.imports_to_match(cur)

def process_players(cur, seasons):
	#truncate imports
	save_player_endpoints(seasons)
	endpoints = get_player_endpoints()

	lock = Lock()
	pool_size = cpu_count()-1
	with Pool(pool_size, initializer=init_child,initargs=(lock,)) as pool:
		pool.starmap(mproc_insert_players, ((key,val) for key, val in endpoints.items()))
	sif.imports_to_player(cur)
	sif.imports_to_player_team(cur)

def process_boxscores(cur):
	lock = Lock()
	pool_size = cpu_count()-1
	matches = get_matches()
	with Pool(pool_size, initializer=init_child,initargs=(lock,)) as pool:
		pool.starmap(mproc_insert_boxscores, matches)
	sif.imports_to_player_performance(cur)

def mproc_insert_boxscores(date, home_abbr):
	'''
    Wrapper function 
		1. Save html of match list
		2. Scrape relevant matches from html 
		3. Save to CSV
		4. Copy CSV to match_import table

	Args: 
		:param season: season given in a tuple
			ex: (2020,)
	:side effect: csv with match stats
    :return: None
    '''
	try:
		date = date.strftime('%Y%m%d')
		bbref_endpoint = date + '0' + home_abbr
		url = "https://www.basketball-reference.com/boxscores/"+bbref_endpoint+".html"
		html = os.path.join(os.getcwd(),"bs4_html/boxscores/"+bbref_endpoint+".html")

		with lock:
			time.sleep(1.5)
			shd.save_html(url, html)
		# with lock:
		# 	logging.info(bbref_endpoint+" html saved")
		

		shd.boxscore_to_csv(html)

		# with lock:
		# 	logging.info(bbref_endpoint+" boxscore saved to csv")
		
		csv = os.path.join(os.getcwd(),"csv/boxscores/"+bbref_endpoint+".csv")
		sif.insert_to_imports(csv)
		# with lock:
		# 	logging.info(bbref_endpoint+" boxscore inserted into imports table")

	except Exception as err:	
		logging.error(bbref_endpoint+" boxscore processing failed")
	return


def get_matches(start_date=datetime.fromisoformat('1900-01-01'),
				end_date=datetime.fromisoformat('2200-12-31')):
	'''
	get_matches: returns all matches between 
	start_date and end_date (inclusive)

	Args: 
		param start_date: in the form of 
			200604010 which represents 2006 april 10
	'''
	conn = db_func.get_conn()
	cur = conn.cursor()
	query = \
		'''SELECT m.date, m.bbref_team
			FROM match as m
			WHERE %s <= m.date
			AND %s >= m.date;'''
	cur.execute(query, (start_date, end_date))
	matches = cur.fetchall()
	conn.close()
	return matches

#Currently only gets all teams for one season
#Returns team abbreviations
def get_teams(season=0):
	#Do not use % operator to format query directly (prone to SQL injections)
	conn = db_func.get_conn()
	cur = conn.cursor()

	if season == 0:
		query = \
		'''SELECT team_abbr
			FROM team_name'''
		cur.execute(query)
	else:
		query = \
			'''SELECT team_abbr
				FROM team_name
				WHERE %s >= active
				AND %s < inactive ;'''
		cur.execute(query, (season, season))
	teams = cur.fetchall()
	teams = [teams[i][0] for i in range(len(teams))]
	conn.close()
	return teams


def get_player_endpoints():
	with open('csv/player_list.csv', newline='') as f:
		reader = csv.reader(f)
		endpoints = {i[0] : i[1] for i in sorted(list(reader), key=lambda x: x[0])}
	return endpoints

def save_player_endpoints(seasons):
	'''
    Wrapper function 
		1. Scrape all endpoints for teams x seasons
		2. Save to CSV

	Args: 
		:param seasons: a list of seasons
	
	:side effect: csv with endpoints 
		(will contain duplicate endpoints)
    :return: None
    '''
	try:
		if not os.path.isdir('csv'):
			os.makedirs('csv')
		with open('csv/player_list.csv', newline='', mode='w+') as f:
			f.truncate()

		pool_size = cpu_count()-1
		if DEBUG:
			pool_size = 1
		lock = Lock()
		
		for s in seasons:
			teams = get_teams(s)
			with Pool(pool_size, initializer=init_child,initargs=(lock,)) as pool:
				pool.starmap(mproc_save_player_endpoints, list(itertools.product(teams, [s])))
		
	except Exception as err:
		logging.exception(traceback.print_exception(*sys.exc_info()))
		sys.exit()


def scrape(start_date=1, end_date=1):
	if not os.path.isdir("logs"):
		os.makedirs("logs")
	logging.basicConfig(level=logging.DEBUG, 
						format= '[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
     					datefmt='%Y-%m-%d %H:%M:%S',
						handlers=[
							logging.FileHandler('logs/'+date.today().strftime("%Y-%m-%d") + '.log'),
							logging.StreamHandler()])	

	try:
		basepath = Path()
		# Load the environment variables
		envars = basepath.cwd().parent.joinpath('db/db.env')
		load_dotenv(envars)
		start = timer()
		conn = db_func.get_conn()
		cur = conn.cursor()
		conn.autocommit = True
		team_list_path = 'target/NBA_Teams.csv'
		seasons_path = 'target/seasons.csv'
		seasons = get_all_seasons(cur)
		
		#insert seasons into db if they dont exist
		if len(seasons) == 0:
			db_func.truncate_imports(cur)
			sif.insert_seasons(seasons_path,cur)
			seasons = get_all_seasons(cur)
		seasons = [(str(seasons[i][0]),)for i in range(len(seasons))]
		
		if DEBUG:
			print(seasons)
			print(len(get_teams(seasons[0][0])))
		
		#insert team names and abbreviations to database
		if len(get_teams(seasons[0][0])) <= 0:
			db_func.truncate_imports(cur)
			sif.insert_to_imports(team_list_path)
			sif.imports_to_team(cur)
			sif.imports_to_team_name(cur)
			conn.commit()


		db_func.truncate_imports(cur)
		process_players(cur, seasons)
		conn.commit()
		db_func.truncate_imports(cur)
		process_matches(cur, seasons)
		conn.commit()
		db_func.truncate_imports(cur)
		process_boxscores(cur)
		sif.player_performance_to_injury(cur)
		conn.commit()
		logging.info("All players inserted")	
			
		end = timer()
		print(f'elapsed time: {end - start}')
	except Exception as err:
		logging.exception(traceback.print_exception(*sys.exc_info()))
		conn.rollback()
		sys.exit()
	finally:
		if (conn):
			conn.close()
			print("PostgreSQL connection is closed")

def update():
	#date range

	#To account for roster changes
	#hash compare player html profiles
	#store the new html files in a temp
	#clear temp folder after use
	return

