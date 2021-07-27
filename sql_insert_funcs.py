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
	try:
		conn = db_func.get_conn()
		cur = conn.cursor()
		with open(csv, 'r') as f: 
			headers = next(f)
			headers = headers.lstrip().rstrip().split(',')
			cur.copy_from(f, 'imports', columns=headers,sep=',')
		conn.commit()
		conn.close()
	except Error as err:
		raise err

def insert_bet_type(csv, cur):
	with open(csv, 'r') as f: 
		headers = next(f)
		headers = headers.lstrip().rstrip().split(',')
		cur.copy_from(f, 'bet_type', columns=headers,sep=',')


def imports_to_bets(cur):
	try:
		query = \
		'''INSERT INTO odds
			(datetime, match_id, spread, decimal_odds, vegas_odds,
			 sportsbook, team_abbr, bet_type_id)
			SELECT im.datetime, m.match_id, im.spread, im.decimal_odds,
				im.vegas_odds, im.sportsbook, im.team_abbr,
				im.bet_type_id
			FROM imports AS im, match AS m, team as t1, team as t2, team as t3, team as t4
			WHERE im.datetime >= m.date - INTERVAL '1 DAY'
				AND im.datetime <= m.date + INTERVAL '2 DAY'

				AND im.home_abbr = t1.team_abbr
				AND t1.team_id = t2.team_id
				AND t2.team_abbr = m.home_abbr

				AND im.away_abbr = t3.team_abbr
				AND t3.team_id = t4.team_id
				AND t4.team_abbr = m.away_abbr

				AND (im.bet_type_id = 1 OR im.bet_type_id = 2)
				AND NOT EXISTS (
					SELECT * FROM odds AS o
					WHERE m.match_id = o.match_id
					AND im.team_abbr = o.team_abbr
					AND im.sportsbook = o.sportsbook
					AND im.bet_type_id = o.bet_type_id
					AND im.datetime = o.datetime 
				);'''
		
		cur.execute(query)
	except Error as err:
		raise err


def imports_to_bets_total(cur):
	try:
		query = \
		'''INSERT INTO odds
			(datetime, match_id, spread, decimal_odds, vegas_odds,
			 sportsbook, team_abbr, bet_type_id)
			SELECT im.datetime, m.match_id, im.spread, im.decimal_odds,
				im.vegas_odds, im.sportsbook, im.team_abbr,
				im.bet_type_id
			FROM imports AS im, match AS m, team as t1, team as t2, team as t3, team as t4
			WHERE
				(im.team_abbr = 'over' OR im.team_abbr = 'under')
				AND im.datetime >= m.date - INTERVAL '1 DAY' 
				AND im.datetime <= m.date + INTERVAL '2 DAY'
				
				AND im.home_abbr = t1.team_abbr
				AND t1.team_id = t2.team_id
				AND t2.team_abbr = m.home_abbr

				AND im.away_abbr = t3.team_abbr
				AND t3.team_id = t4.team_id
				AND t4.team_abbr = m.away_abbr

				AND im.bet_type_id = 3
				AND NOT EXISTS (
					SELECT * FROM odds AS o
					WHERE m.match_id = o.match_id
					AND im.team_abbr = o.team_abbr
					AND im.sportsbook = o.sportsbook
					AND im.bet_type_id = o.bet_type_id
					AND im.datetime = o.datetime );'''
		
		cur.execute(query)
	except Error as err:
		raise err



def imports_to_player_performance(cur):
	try:
		query = \
		'''INSERT INTO player_performance
			(player_id, match_id, team_abbr, inactive,
			ts_pct, efg_pct, threepar, ftr, orb_pct,
			drb_pct, trb_pct, ast_pct, stl_pct, blk_pct,
			tov_pct, usg_pct, ortg, drtg, bpm, starter, date, 
			fg, fga, fg_pct, threep, threepa,
			threep_pct, ft, fta, ft_pct, orb, drb, trb, ast,
			stl, blk, tov, pf, pts, pm)
			SELECT p.player_id, m.match_id, im.team_abbr, 
				im.inactive, im.ts_pct, im.efg_pct, im.threepar, 
				im.ftr, im.orb_pct, im.drb_pct, im.trb_pct, 
				im.ast_pct, im.stl_pct, im.blk_pct,
				im.tov_pct, im.usg_pct, im.ortg, im.drtg, 
				im.bpm, im.starter, im.date, im.fg, im.fga, im.fg_pct, 
				im.threep, im.threepa, im.threep_pct, 
				im.ft, im.fta, im.ft_pct, im.orb, im.drb, im.trb, im.ast,
				im.stl, im.blk, im.tov, im.pf, im.pts, im.pm
			FROM imports AS im, player AS p, match AS m, 
				player_team AS pt, season AS s
			WHERE im.player_name = p.player_name
				AND p.player_id = pt.player_id
				AND im.team_abbr = pt.team_abbr
				AND s.start_date <= im.date 
				AND s.end_date >= im.date 
				AND s.season = pt.season
				AND im.date = m.date
				AND (im.team_abbr = m.home_abbr
					OR im.team_abbr = m.away_abbr)
			AND NOT EXISTS
				(SELECT *
					FROM player_performance AS pp
					WHERE (im.team_abbr = m.home_abbr
							OR im.team_abbr = m.away_abbr)
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
			(team_name, team_abbr, home_arena_elevation,
			 created, inactive)
			SELECT im.team_name, im.team_abbr, im.home_arena_elevation,
				im.created, inactive.home
			FROM imports as im
			WHERE NOT EXISTS
				(SELECT *
					FROM team AS t
					WHERE t.team_abbr = im.team_abbr
					);'''
		cur.execute(query)
	except Error as err:
		raise err

def imports_to_match(cur):
	try:
		query = \
		'''INSERT INTO match
			(date, away_pts, home_pts, away_abbr, home_abbr, 
			playoff_game, elevation)
			SELECT im.date, im.away_pts, im.home_pts,
				im.away_abbr, im.home_abbr, im.playoff_game,
				im.elevation
			FROM imports as im
			WHERE NOT EXISTS
				(SELECT *
					FROM match AS m
					WHERE m.date = im.date
						AND m.away_abbr = im.away_abbr
						AND m.home_abbr = im.home_abbr);'''
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
			FROM imports AS im
			WHERE NOT EXISTS
				(SELECT *
					FROM player AS p
					WHERE p.bbref_endpoint = im.bbref_endpoint);'''
		cur.execute(query)
	except Error as err:
		raise err

def imports_to_player_team(cur):
	try:
		query = \
		'''INSERT INTO player_team
			(player_id, team_abbr, season, age, lg, 
			pos, g, gs, mp, fg, fga, fg_pct, threep,
			threepa, threep_pct, twop, twopa, twop_pct,
			efg_pct, ft, fta, ft_pct, orb, drb, trb, 
			ast, stl, blk, tov, pf, pts)
			SELECT DISTINCT 
				p.player_id, im.team_abbr, im.season, im.age, im.lg, 
				im.pos, im.g, im.gs, im.mp, im.fg, im.fga,
				im.fg_pct, im.threep, im.threepa, im.threep_pct, 
				im.twop, im.twopa, im.twop_pct, im.efg_pct, im.ft, 
				im.fta, im.ft_pct, im.orb, im.drb, im.trb, 
				im.ast, im.stl, im.blk, im.tov, im.pf, im.pts
			FROM imports AS im, player AS p, team AS t
			WHERE NOT EXISTS
				(SELECT *
					FROM player_team AS pt
					WHERE p.player_id = pt.player_id
					AND im.bbref_endpoint = p.bbref_endpoint
					AND im.team_abbr = pt.team_abbr
					AND im.season = pt.season)
			AND im.team_abbr = t.team_abbr
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
