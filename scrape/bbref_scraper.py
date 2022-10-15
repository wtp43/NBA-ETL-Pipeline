import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')) + '/db/')
import sql_func as sif
import db_func
import scrape_historical_data as shd

import istarmap
import tqdm
from psycopg2 import Error
import logging
from multiprocessing import Pool, cpu_count, Lock
from timeit import default_timer as timer
import traceback
from datetime import date, datetime
import itertools
import csv
from pandas.core.frame import DataFrame
import time


POOL_SIZE = 15

DEBUG = True

def init_child(lock_, connection_required=False):
	global lock
	lock = lock_
	if connection_required:
		global conn, cur
		conn = db_func.get_conn()
		cur = conn.cursor()

def get_all_seasons(cur):
	try:
		select_matches_query = \
		'''SELECT season
		FROM season;'''
		cur.execute(select_matches_query)
		return list(cur.fetchall())
	except Error as err:
		raise err


def save_player_endpoints(team,season):
	'''
    save_player_endpoints 

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
		if not os.path.isfile(html) or os.stat(html).st_size ==0:
			with lock:
				time.sleep(1.5)
				shd.save_html(url, html)
		
		df = shd.get_endpoints_df(html)
		csv = 'csv/player_list.csv'
		with lock:
			df.to_csv(csv, mode='a+',index=False, header=False)

	except Exception as err:
		logging.error("save_player_endpoints error: {}, {}".format(team, season))
		raise err

def insert_matches(season):
	'''
	Insert all matches given a season
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
		if not os.path.isfile(html) or os.stat(html).st_size ==0:
			with lock:
				time.sleep(1.5)
				shd.save_html(url, html)
		
		shd.match_list_to_csv(html)
		csv = os.path.join(os.getcwd(),"csv/match_lists/" + season \
			+ "_match_list.csv")
		sif.copy_to_imports(cur,csv)	
		conn.commit()
	except Exception as err:
		raise err


def mproc_matches(cur, seasons):
	db_func.truncate_imports(cur)
	lock = Lock()
	print(f'starting computations on {POOL_SIZE} cores')
	with Pool(POOL_SIZE, initializer=init_child,initargs=(lock,True)) as pool:
		for _ in tqdm.tqdm(pool.map(insert_matches, seasons, 
							chunksize=len(seasons)//POOL_SIZE),
                           total=len(seasons)):		   
				pass
	sif.imports_to_match(cur)


def insert_player(bbref_endpoint, player_name):
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
		if not os.path.isfile(html) or os.stat(html).st_size ==0:
			with lock:
				time.sleep(1.5)
				shd.save_html(url, html)

		err = shd.player_data_to_csv(html, bbref_endpoint, player_name)
		if err:
			with lock:
				print(html, bbref_endpoint, player_name)
				logging.info('bbref_endpoint: {}, player_name: {} NO DATA FOUND'
			.format(bbref_endpoint, player_name))
			return 
			
		#Insert player data into imports
		file_path = 'csv' + bbref_endpoint[:-5] + '.csv'
		sif.copy_to_imports(cur, file_path)
		conn.commit()
		
	except Exception as err:
		raise err


def mproc_players(cur, seasons):
	#truncate imports
	mproc_player_endpoints(seasons)
	endpoints = get_player_endpoints()
	lock = Lock()

	with Pool(POOL_SIZE, initializer=init_child,initargs=(lock,True)) as pool:
		for _ in tqdm.tqdm(pool.starmap(insert_player, 
							((key,val) for key, val in endpoints.items()),
							chunksize=len(endpoints)//POOL_SIZE),
                           total=len(endpoints)):		   
				pass
	sif.imports_to_player(cur)
	sif.imports_to_player_team(cur)


def mproc_boxscores(cur):
	lock = Lock()
	matches = get_matches()
	with Pool(POOL_SIZE, initializer=init_child,initargs=(lock,True)) as pool:
		for _ in tqdm.tqdm(pool.starmap(insert_boxscores, matches, 
							chunksize=len(matches)//POOL_SIZE),
							total=len(matches)):
			pass			
	sif.imports_to_player_performance(cur)


def insert_boxscores(date, home_abbr):
	try:
		date = date.strftime('%Y%m%d')
		bbref_endpoint = date + '0' + home_abbr
		url = "https://www.basketball-reference.com/boxscores/"+bbref_endpoint+".html"
		html = os.path.join(os.getcwd(),"bs4_html/boxscores/"+bbref_endpoint+".html")
		if not os.path.isfile(html) or os.stat(html).st_size ==0:
			with lock:
				time.sleep(1.5)
				shd.save_html(url, html)
		shd.boxscore_to_csv(html)
		csv = os.path.join(os.getcwd(),"csv/boxscores/"+bbref_endpoint+".csv")
		sif.copy_to_imports(cur,csv)
		conn.commit()
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
def get_teams(cur, season=0):
	#Do not use % operator to format query directly (prone to SQL injections)
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
	return teams


def get_player_endpoints():
	with open('csv/player_list.csv', newline='') as f:
		reader = csv.reader(f)
		endpoints = {i[0] : i[1] for i in sorted(list(reader), key=lambda x: x[0])}
	return endpoints


def mproc_player_endpoints(seasons):
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
		lock = Lock()
		conn = db_func.get_conn()
		cur = conn.cursor()
		for s in seasons:
			teams = get_teams(cur, s)
			with Pool(POOL_SIZE, initializer=init_child,initargs=(lock,)) as pool:
				for _ in tqdm.tqdm(pool.starmap(save_player_endpoints, 
									list(itertools.product(teams, [s]))),
									total=len(teams)):
					pass
		conn.close()
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
		start = timer()
		# Use getconn() method to Get Connection from connection pool
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
			print(len(get_teams(cur,seasons[0][0])))
		
		#insert team names and abbreviations to database
		if len(get_teams(cur, seasons[0][0])) <= 0:
			db_func.truncate_imports(cur)
			sif.copy_to_imports(cur, team_list_path)
			sif.imports_to_team(cur)
			sif.imports_to_team_name(cur)
			conn.commit()

		db_func.truncate_imports(cur)
		logging.info("Inserting players...")
		mproc_players(cur, seasons)
		conn.commit()
		logging.info("All players inserted")	
		db_func.truncate_imports(cur)
		logging.info("Inserting matches...")
		mproc_matches(cur, seasons)
		conn.commit()
		logging.info("All matches inserted")	
		db_func.truncate_imports(cur)
		logging.info("Inserting boxscores...")
		mproc_boxscores(cur)
		sif.player_performance_to_injury(cur)
		conn.commit()
			
		end = timer()
		print(f'elapsed time: {end - start}')
	except Exception as err:
		logging.exception(traceback.print_exception(*sys.exc_info()))
		conn.rollback()
		sys.exit()
	finally:
		conn.close()
		print("PostgreSQL connection is closed")

def update():
	#date range

	#To account for roster changes
	#hash compare player html profiles
	#store the new html files in a temp
	#clear temp folder after use
	#get last date from db
	return

