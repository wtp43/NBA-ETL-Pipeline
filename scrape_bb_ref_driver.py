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
from pandas.core.frame import DataFrame



DEBUG = False

def init_child(lock_):
	global lock
	global conn
	conn = db_func.get_conn()
	lock = lock_



def insert_player_performance(csv_file, ticker, cur):
	try:
		headers = ['player_name', 'sp', 'ts_p', 'efg_p', 'three_par', 'ftr', 'orb_p',
       'drb_p', 'trb_p', 'ast_p', 'stl_p', 'blk_p', 'tov_p', 'usg_p', 'ortg',
       'drtg', 'bpm', 'team_name', 'starter', 'match_id', 'date', 'fg', 'fga',
       'fg_p', 'three_p', 'three_pa', 'three_p_p', 'ft', 'fta', 'ft_p', 'orb',
       'drb', 'trb', 'ast', 'stl', 'blk', 'tov', 'pf', 'pts', 'pm']
		with open(csv_file, 'r') as f: 
			next(f)
			cur.copy_from(f, 'player_performance_imports', columns=headers,sep=',')
	except Exception as err:
		raise err

def insert_matches(csv_file, cur):
	try:
		headers = ['date', 'away', 'away_pts', 'home', 'home_pts']
		with open(csv_file, 'r') as f: 
			next(f)
			cur.copy_from(f, 'match_imports', columns=headers,sep=',')
	except Exception as err:
		raise err

def insert_teams(csv_file, cur):
	headers = ['symbol', 'name']
	with open(csv_file, 'r') as f: 
		next(f)
		cur.copy_from(f, 'team', columns=headers,sep=',')

def get_match_list_csvs(seasons):
	try:
		all_files = []
		for i in range(len(seasons)):
			files = ["csv/"+seasons[i] + '/' + f for f in os.listdir("csv/" + seasons[i]) 
					if (os.path.isfile(os.path.join("csv/"+seasons[i] + '/',f)) and f == 'match_list.csv')]
			all_files +=files
	except Exception as err:
		raise err
	return all_files

def match_imports_to_match(cur):
	try:
		insert_matches_query = \
		'''INSERT INTO match
			(date, away_pts, home_pts, away, home, elevation)
			SELECT im.date, im.away_pts, im.home_pts,
				im.away, im.home, im.elevation
			FROM match_imports as im
			WHERE NOT EXISTS
				(SELECT *
					FROM match AS m, match_imports as im
					WHERE m.date = im.date
						AND m.away = im.away
						AND m.home = im.home);'''
		cur.execute(insert_matches_query)
	except Error as err:
		raise err

def player_imports_to_player(cur):
	try:
		insert_matches_query = \
		'''INSERT INTO player
			()
			SELECT im.date, im.away_pts, im.home_pts,
				im.away, im.home, im.elevation
			FROM player_imports as im
			WHERE NOT EXISTS
				(SELECT *
					FROM match AS m, match_imports as im
					WHERE m.date = im.date
						AND m.away = im.away
						AND m.home = im.home);'''
		cur.execute(insert_matches_query)
	except Error as err:
		raise err


def get_all_matches(cur):
	try:
		select_matches_query = \
		'''SELECT m.date, m.home, m.match_id
		FROM match as m;'''
		cur.execute(select_matches_query)
		return list(cur.fetchall())
	except Error as err:
		raise err

def get_all_seasons(cur):
	try:
		select_matches_query = \
		'''SELECT year
		FROM season;'''
		cur.execute(select_matches_query)
		return list(cur.fetchall())
	except Error as err:
		raise err

def insert_seasons(cur):
	try:
		seasons = [[i] for i in(range(2005,2022))]
		cur.executemany('''INSERT INTO season (year)
							VALUES (%s)
							ON CONFLICT (year) DO NOTHING;''', seasons)
	except Error as err:
		conn.rollback()
		cur.close()
		raise err



def mproc_save_player_endpoints(team,season):
	'''
    mproc_save_player_endpoints 

	Args: 
		:param team: endpoint of player on bbref
			ex: /players/b/bareajo01.html
	
	:side effect: csv with match stats
    :return: None
    '''
	try:

		# with lock:
		# 	logging.info("DB connection established")
		print(season)
		url = 'https://www.basketball-reference.com/teams/' + team + \
			'/' + season + '.html'
		html = os.path.join(os.getcwd(),"bs4_html/roster/" + season + \
			"/" +team+".html")
		#print(url, html)
		shd.save_html(url, html)
		df = shd.get_endpoints_df(html)

		csv = 'csv/player_list.csv'
		with lock:
			df.to_csv(csv, mode='a+',index=False, header=False)

	except Exception as err:
		raise err

def mproc_insert_players(player):
	'''
    Wrapper function 
		1. Save html of match list
		2. Scrape relevant matches from html 
		3. Save to CSV
		4. Copy CSV to match_import tabl

	Args: 
		:param match_html: html of the given match 
			bs4_html/boxscores/date+hometeam.html
	
	:side effect: csv with match stats
    :return: None
    '''
	try:
		conn = db_func.get_conn()
		cur = conn.cursor()
		url = "https://www.basketball-reference.com/leagues/NBA_" + player \
				+ ".html"
		html = os.path.join(os.getcwd(),"bs4_html/player/" \
			+ "/" +player+".html")
		shd.save_html(url, html)

		with lock:
			logging.info(player+" season html saved")
		
		#scrape relevant info to csv
		#shd.append_player_endpoints_to_csv(html)
		with lock:
			logging.info(player+" season matches saved to csv")

		with lock:
			logging.info(player +": season matches inserted into match_imports")
		conn.commit()

		
	except Exception as err:
		if conn is not None:
			conn.rollback()
		cur = None
		raise err
	finally:
		if conn:
			conn.close()	


def mproc_insert_matches(season):
	'''
    Wrapper function 
		1. Save html of match list
		2. Scrape relevant matches from html 
		3. Save to CSV
		4. Copy CSV to match_import table

	Args: 
		:param match_html: html of the given match 
			bs4_html/boxscores/date+hometeam.html
	
	:side effect: csv with match stats
    :return: None
    '''
	try:
		season = season[0]

		conn = db_func.get_conn()
		cur = conn.cursor()
		
		url = "https://www.basketball-reference.com/leagues/NBA_" + season \
				+ "_games.html"
		html = os.path.join(os.getcwd(),"bs4_html/match_list/" \
			+ "/" +season+".html")
		shd.save_html(url, html)

		with lock:
			logging.info(season+" season html saved")
		
		shd.match_list_to_csv(html)
		with lock:
			logging.info(season+" season matches saved to csv")
		
		csv = os.path.join(os.getcwd(),"csv/" + season + "/" \
			+ "/match_list.html")
		insert_matches(csv, cur)

		with lock:
			logging.info(season +": season matches inserted into match_imports")
		conn.commit()

		
	except Exception as err:
		if conn is not None:
			conn.rollback()
		cur = None
		raise err
	finally:
		if conn:
			conn.close()

def process_matches(cur, seasons):
	lock = Lock()
	pool_size = cpu_count()
	print(f'starting computations on {pool_size} cores')
	with Pool(pool_size, initializer=init_child,initargs=(lock,)) as pool:
		pool.map(mproc_insert_matches, seasons)
	match_imports_to_match(cur)

def process_players(cur, seasons):

	save_player_endpoints(seasons)

	lock = Lock()
	pool_size = cpu_count()
	#with Pool(pool_size, initializer=init_child,initargs=(lock,)) as pool:
		# pool.map(mproc_insert_players, seasons)
		# player_imports_to_player(cur)

def get_teams():
	conn = db_func.get_conn()
	cur = conn.cursor()
	query = \
		'''SELECT symbol
			FROM team;'''
	cur.execute(query)
	teams = cur.fetchall()
	teams = [teams[i][0] for i in range(len(teams))]
	conn.close()
	return teams


def get_player_endpoints():
	with open('csv/player_list.csv', newline='') as f:
		reader = csv.reader(f)
		endpoints = set((list(reader)))
	return endpoints

def save_player_endpoints(seasons):
	'''
    Wrapper function 
		1. 
		2. 
		3. Save to CSV

	Args: 
		:param match_html: html of the given match 
			bs4_html/boxscores/date+hometeam.html
	
	:side effect: csv with match stats
    :return: None
    '''
	try:
		
		with open('csv/player_list.csv', newline='', mode='w+') as f:
			f.truncate()
		teams = get_teams()
		pool_size = cpu_count()
		if DEBUG:
			pool_size = 1
		lock = Lock()
		print(seasons)
		with Pool(pool_size, initializer=init_child,initargs=(lock,)) as pool:
			pool.starmap(mproc_save_player_endpoints, list(itertools.product(teams, seasons)))
		

	except Exception as err:
		logging.exception(traceback.print_exception(*sys.exc_info()))
		sys.exit()





















def run_scraper():
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

		team_list_path = 'db_src/NBA_Teams.csv'
		insert_seasons(cur)
		seasons = get_all_seasons(cur)
		seasons = [(str(seasons[i][0]),)for i in range(len(seasons))]
		insert_teams(team_list_path,cur)

		db_func.truncate_imports(conn)
		start = timer()
		
		process_matches(cur, seasons)
		logging.info("match table updated with match_imports")	
			
	
		
		conn.commit()
		end = timer()
		print(f'elapsed time: {end - start}')
	except Exception as err:
		logging.exception(traceback.print_exception(*sys.exc_info()))
		sys.exit()
	finally:
		if (conn):
			conn.close()
			print("PostgreSQL connection is closed")


def main():
	run_scraper()

if __name__ == '__main__':
	main()
