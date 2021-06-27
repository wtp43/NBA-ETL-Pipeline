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


def insert_teams(csv, cur):
	headers = ['symbol', 'name', 'home_arena_elevation', 'created', 'inactive']
	with open(csv, 'r') as f: 
		next(f)
		cur.copy_from(f, 'team', columns=headers,sep=',')

def insert_to_imports(csv, cur):
	with open(csv, 'r') as f: 
		headers = next(f)
		headers = headers.lstrip().rstrip().split(',')
		print(headers)
		print(len(headers))
		cur.copy_from(f, 'imports', columns=headers,sep=',')


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

def insert_seasons(cur):
	try:
		seasons = [[i] for i in(range(2005,2022))]
		cur.executemany('''INSERT INTO season (year)
							VALUES (%s)
							ON CONFLICT (year) DO NOTHING;''', seasons)
	except Error as err:
		cur.close()
		raise err
