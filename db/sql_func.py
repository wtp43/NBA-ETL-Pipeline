import db_func as db_func
from psycopg2 import Error


def copy_to_imports(cur,csv):
	with open(csv, 'r') as f:
		headers = next(f)
		headers = headers.lstrip().rstrip().split(',')
		cur.copy_from(f, 'imports', columns=headers,sep=',')

def insert_bet_type(csv, cur):
	with open(csv, 'r') as f: 
		headers = next(f)
		headers = headers.lstrip().rstrip().split(',')
		cur.copy_from(f, 'bet_type', columns=headers,sep=',')

def fill_missing_odds(cur):
	try:
		query = \
		'''INSERT INTO odds
			(datetime, match_id, vegas_odds, sportsbook, team_id, bet_type_id,
				decimal_odds)
			SELECT im.datetime, m.match_id, im.vegas_odds, im.sportsbook, 
				t1.team_id, im.bet_type_id, im.decimal_odds
			FROM imports AS im, match AS m, team_name as t1
			WHERE  im.datetime >= m.date - INTERVAL '1 DAY'
				AND im.datetime <= m.date + INTERVAL '2 DAY'
				AND im.team_abbr = t1.team_abbr
				AND (t1.team_id = m.home_id OR 
					t1.team_id = m.away_id)
				AND (im.bet_type_id = 1)
				AND NOT EXISTS (
					SELECT * FROM odds AS o
					WHERE m.match_id = o.match_id
						AND t1.team_id = o.team_id
						AND im.bet_type_id = 1
				);'''
		cur.execute(query)
	except Error as err:
		raise err

def imports_to_bets(cur):
	try:
		query = \
		'''INSERT INTO odds
			(datetime, match_id, spread, decimal_odds, vegas_odds,
			 sportsbook, team_id, bet_type_id)
			SELECT im.datetime, m.match_id, im.spread, im.decimal_odds,
				im.vegas_odds, im.sportsbook, t3.team_id,
				im.bet_type_id
			FROM imports AS im, match AS m, team_name as t1, team_name as t2, 
			team_name as t3
			WHERE im.datetime >= m.date - INTERVAL '1 DAY'
				AND im.datetime <= m.date + INTERVAL '2 DAY'

				AND im.home_abbr = t1.team_abbr
				AND t1.team_id = m.home_id

				AND im.away_abbr = t2.team_abbr
				AND t2.team_id = m.away_id
				AND im.team_abbr = t3.team_abbr
				AND (t3.team_id = t2.team_id
					OR t3.team_id = t1.team_id)

				AND (im.bet_type_id = 1 OR im.bet_type_id = 2)
				AND NOT EXISTS (
					SELECT * FROM odds AS o
					WHERE m.match_id = o.match_id
					AND t3.team_id = o.team_id
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
			 sportsbook, over_under, bet_type_id)
			SELECT im.datetime, m.match_id, im.spread, im.decimal_odds,
				im.vegas_odds, im.sportsbook, im.team_abbr,
				im.bet_type_id
			FROM imports AS im, match AS m, team_name as t1, team_name as t2
			WHERE
				(im.team_abbr = 'over' OR im.team_abbr = 'under')
				AND im.datetime >= m.date - INTERVAL '1 DAY' 
				AND im.datetime <= m.date + INTERVAL '2 DAY'
				
				AND im.home_abbr = t1.team_abbr
				AND t1.team_id = m.home_id

				AND im.away_abbr = t2.team_abbr
				AND t2.team_id = m.away_id

				AND im.bet_type_id = 3
				AND NOT EXISTS (
					SELECT * FROM odds AS o
					WHERE m.match_id = o.match_id
					AND im.team_abbr = o.over_under
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
			(player_id, match_id, team_id, inactive,
			ts_pct, efg_pct, threepar, ftr, orb_pct,
			drb_pct, trb_pct, ast_pct, stl_pct, blk_pct,
			tov_pct, usg_pct, ortg, drtg, bpm, starter,
			fg, fga, fg_pct, threep, threepa,
			threep_pct, ft, fta, ft_pct, orb, drb, trb, ast,
			stl, blk, tov, pf, pts, pm, sp)
			SELECT p.player_id, m.match_id, t.team_id, 
				im.inactive, im.ts_pct, im.efg_pct, im.threepar, 
				im.ftr, im.orb_pct, im.drb_pct, im.trb_pct, 
				im.ast_pct, im.stl_pct, im.blk_pct,
				im.tov_pct, im.usg_pct, im.ortg, im.drtg, 
				im.bpm, im.starter, im.fg, im.fga, im.fg_pct, 
				im.threep, im.threepa, im.threep_pct, 
				im.ft, im.fta, im.ft_pct, im.orb, im.drb, im.trb, im.ast,
				im.stl, im.blk, im.tov, im.pf, im.pts, im.pm,
				im.sp
			FROM imports AS im, player AS p, match AS m, 
				player_team AS pt, season AS s, team_name AS t
			WHERE im.player_name = p.player_name
				AND p.player_id = pt.player_id
				AND im.team_abbr = t.team_abbr
				AND t.team_id = pt.team_id
				AND s.start_date <= im.date 
				AND s.end_date >= im.date 
				AND s.season = pt.season
				AND im.date = m.date
				AND (t.team_id = m.home_id
					OR t.team_id = m.away_id)
			AND NOT EXISTS
				(SELECT *
					FROM player_performance AS pp
					WHERE (t.team_id = m.home_id
							OR t.team_id = m.away_id)
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
			(team_id)
			SELECT DISTINCT(im.team_id)
			FROM imports as im
			WHERE NOT EXISTS
				(SELECT *
					FROM team AS t
					WHERE t.team_id = im.team_id
					);'''
		cur.execute(query)
	except Error as err:
		raise err

def imports_to_team_name(cur):
	try:
		query = \
		'''INSERT INTO team_name
			(team_id, team_name, team_abbr,
			 active, inactive)
			SELECT im.team_id, im.team_name, im.team_abbr,
				im.active, im.inactive
			FROM imports as im
			WHERE NOT EXISTS
				(SELECT *
					FROM team_name AS t
					WHERE t.team_abbr = im.team_abbr
						AND t.team_id = im.team_id
						AND t.active = im.active
						AND t.inactive = im.inactive
					);'''
		cur.execute(query)
	except Error as err:
		raise err

def imports_to_match(cur):
	try:
		query = \
		'''INSERT INTO match
			(date, away_pts, home_pts, home_id, away_id, 
			playoff_game, elevation, bbref_team)
			SELECT im.date, im.away_pts, im.home_pts,
				t1.team_id, t2.team_id, 
				im.playoff_game, im.elevation, im.home_abbr
			FROM imports as im, team_name as t1, team_name as t2
			WHERE im.home_abbr = t1.team_abbr 
				AND	im.away_abbr = t2.team_abbr
				AND NOT EXISTS
				(SELECT *
					FROM match AS m
					WHERE m.date = im.date
						AND m.home_id = t1.team_id 
						AND m.away_id = t2.team_id );'''
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
			(player_id, team_id, season, age, lg, 
			pos, g, gs, mp, fg, fga, fg_pct, threep,
			threepa, threep_pct, twop, twopa, twop_pct,
			efg_pct, ft, fta, ft_pct, orb, drb, trb, 
			ast, stl, blk, tov, pf, pts)
			SELECT DISTINCT 
				p.player_id, t.team_id, im.season, im.age, im.lg, 
				im.pos, im.g, im.gs, im.mp, im.fg, im.fga,
				im.fg_pct, im.threep, im.threepa, im.threep_pct, 
				im.twop, im.twopa, im.twop_pct, im.efg_pct, im.ft, 
				im.fta, im.ft_pct, im.orb, im.drb, im.trb, 
				im.ast, im.stl, im.blk, im.tov, im.pf, im.pts
			FROM imports AS im, player AS p, team_name AS t
			WHERE NOT EXISTS
				(SELECT *
					FROM player_team AS pt
					WHERE p.player_id = pt.player_id
					AND im.bbref_endpoint = p.bbref_endpoint
					AND t.team_id = pt.team_id
					AND im.season = pt.season)
			AND im.team_abbr = t.team_abbr
			AND im.bbref_endpoint = p.bbref_endpoint;'''
			
		cur.execute(query)
	except Error as err:
		raise err	

def player_performance_to_injury(cur):
	try:
		query = \
		'''INSERT INTO injury
			(match_id, player_id)
			SELECT pf.match_id, pf.player_id
			FROM player_performance AS pf
			WHERE 
			pf.inactive = 1 AND
			NOT EXISTS
				(SELECT *
					FROM injury AS i
					WHERE i.match_id = pf.match_id AND
					i.player_id = pf.player_id);'''
		cur.execute(query)
	except Error as err:
		raise err


def insert_seasons(csv, cur):
	with open(csv, 'r') as f: 
		headers = next(f)
		headers = headers.lstrip().rstrip().split(',')
		cur.copy_from(f, 'season', columns=headers,sep=',')
