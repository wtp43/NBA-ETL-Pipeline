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
	with open(csv, 'r') as f: 
		headers = next(f)
		headers = headers.lstrip().rstrip().split(',')
		cur.copy_from(f, 'team', columns=headers,sep=',')

def insert_to_imports(csv):
	conn = db_func.get_conn()
	conn.commit()
	cur = conn.cursor()
	with open(csv, 'r') as f: 
		headers = next(f)
		headers = headers.lstrip().rstrip().split(',')
		#print(headers)
		cur.copy_from(f, 'imports', columns=headers,sep=',')
	conn.commit()
	conn.close()



def imports_to_player_performance(cur):
	try:
		query = \
		'''INSERT INTO player_performance
			(player_id, match_id, team_id, inactive,
			ts_pct, efg_pct, threepar, ftr, orb_pct,
			drb_pct, trb_pct, ast_pct, stl_pct, blk_pct,
			tov_pct, usg_pct, ortg, drtg, bpm, starter, date, 
			fg, fga, fg_pct, threep, threepa,
			threep_pct, ft, fta, ft_pct, orb, drb, trb, ast,
			stl, blk, tov, pf, pts, pm)
			SELECT p.player_id, m.match_id, im.team_id, 
				im.inactive, im.ts_pct, im.efg_pct, im.threepar, 
				im.ftr, im.orb_pct, im.drb_pct, im.trb_pct, 
				im.ast_pct, im.stl_pct, im.blk_pct,
				im.tov_pct, im.usg_pct, im.ortg, im.drtg, 
				im.bpm, im.starter, im.date, im.fg, im.fga, im.fg_pct, 
				im.threep, im.threepa, im.threep_pct, 
				im.ft, im.fta, im.ft_pct, im.orb, im.drb, im.trb, im.ast,
				im.stl, im.blk, im.tov, im.pf, im.pts, im.pm
			FROM imports AS im, player AS p, match AS m, 
				player_team AS pt, season as s
			WHERE im.player_name = p.player_name
				AND p.player_id = pt.player_id
				AND im.team_id = pt.team_id
				AND s.start_date <= im.date 
				AND s.end_date >= im.date 
				AND s.season = pt.season
				AND im.date = m.date
				AND (im.team_id = m.home_id
					OR im.team_id = m.away_id)
			AND NOT EXISTS
				(SELECT *
					FROM imports AS im, 
						player AS p, player_performance AS pp
					WHERE (im.team_id = m.home_id
							OR im.team_id = m.away_id)
						AND im.date = m.date
						AND m.match_id = pp.match_id
						AND im.player_name = p.player_name
						AND p.player_id = pp.player_id
					);'''
		cur.execute(query)
	except Error as err:
		raise err	

def imports_to_team(cur):
	try:
		query = \
		'''INSERT INTO team
			(team_name, team_id, home_arena_elevation,
			 created, inactive)
			SELECT im.team_name, im.team_id, im.home_arena_elevation,
				im.created, inactive.home
			FROM imports as im
			WHERE NOT EXISTS
				(SELECT *
					FROM team AS t, imports as im
					WHERE t.team_id = im.team_id
					);'''
		cur.execute(query)
	except Error as err:
		raise err

def imports_to_match(cur):
	try:
		query = \
		'''INSERT INTO match
			(date, away_pts, home_pts, away_id, home_id, 
			playoff_game, elevation)
			SELECT im.date, im.away_pts, im.home_pts,
				im.away_id, im.home_id, im.playoff_game,
				im.elevation
			FROM imports as im
			WHERE NOT EXISTS
				(SELECT *
					FROM match AS m, imports as im
					WHERE m.date = im.date
						AND m.away_id = im.away_id
						AND m.home_id = im.home_id);'''
		cur.execute(query)
	except Error as err:
		raise err

def imports_to_player(cur):
	try:
		query = \
		'''INSERT INTO player
			(bbref_endpoint, player_name)
			SELECT DISTINCT ON (im.bbref_endpoint)
				im.bbref_endpoint, im.player_name
			FROM imports as im
			WHERE NOT EXISTS
				(SELECT *
					FROM player AS p, imports as im
					WHERE p.bbref_endpoint = im.bbref_endpoint);'''
		cur.execute(query)
	except Error as err:
		raise err

def imports_to_player_team(cur):
	try:
		query = \
		'''INSERT INTO player_team
			(player_id, team_id, season, age, lg, 
			pos, g, gs, mp, fg, fga, fg_pct, threep,
			threepa, threep_pct, twop, twopa, twop_pct,
			efg_pct, ft, fta, ft_pct, orb, drb, trb, 
			ast, stl, blk, tov, pf, pts)
			SELECT DISTINCT 
				p.player_id, im.team_id, im.season, im.age, im.lg, 
				im.pos, im.g, im.gs, im.mp, im.fg, im.fga,
				im.fg_pct, im.threep, im.threepa, im.threep_pct, 
				im.twop, im.twopa, im.twop_pct, im.efg_pct, im.ft, 
				im.fta, im.ft_pct, im.orb, im.drb, im.trb, 
				im.ast, im.stl, im.blk, im.tov, im.pf, im.pts
			FROM imports as im, player as p, team as t
			WHERE NOT EXISTS
				(SELECT *
					FROM player_team AS pt, imports as im,
						player as p
					WHERE p.player_id = pt.player_id
					AND im.bbref_endpoint = p.bbref_endpoint
					AND im.team_id = pt.team_id
					AND im.season = pt.season)
			AND im.team_id = t.team_id
			AND im.bbref_endpoint = p.bbref_endpoint;'''
			
		cur.execute(query)
	except Error as err:
		raise err	

def insert_seasons(csv, cur):
	with open(csv, 'r') as f: 
		headers = next(f)
		headers = headers.lstrip().rstrip().split(',')
		print(headers)
		cur.copy_from(f, 'season', columns=headers,sep=',')
