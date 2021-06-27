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
		#test_process_players(cur,seasons )
		shd.save_html('https://www.basketball-reference.com/players/j/jacksbo01.html',
			'bs4_html/players/j/jacksbo01.html')
		shd.player_data_to_csv('bs4_html/players/j/jacksbo01.html', 
		'/players/j/jacksbo01.html')
		sif.insert_to_imports('csv/players/j/jacksbo01.csv', cur)
		# team_list_path = 'db_src/NBA_Teams.csv'
		# sbrd.insert_teams(team_list_path,cur)
		conn.commit()
	
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


def test_get_teams():
	for i in range(2010, 2021):
		teams = sbrd.get_teams(i)
		assert len(teams) == 30
		print(teams)
		print(i, len(teams))


if __name__ == '__main__':
	main()