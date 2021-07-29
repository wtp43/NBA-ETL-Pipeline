from datetime import datetime
import pandas as pd
pd.options.display.max_columns = None
pd.options.display.max_rows = None
import time

from pysbr import EventsByDateRange, CurrentLines, NBA, Sportsbook, Team, EventsByEventIds
from datetime import datetime, timedelta
import db_func
import os
from os import listdir
from os.path import isfile, join
from tqdm import tqdm
import sql_insert_funcs as sif
import re

team_id = {'Atlanta': 'ATL', 'Boston':'BOS', 'Brooklyn': 'BRK', 'New Jersey':'BRK', 
			'Chicago':'CHI', 'Cleveland':'CLE', 'Dallas':'DAL', 'Denver':'DEN', 
			'Golden State':'GSW', 'Houston':'HOU', 'Indiana':'IND', 'L.A. Clippers': 'LAC',
			'L.A. Lakers':'LAL', 'Memphis':'MEM', 'Miami':'MIA', 'Milwaukee':'MIL', 
			'Minnesota':'MIN', 'New Orleans':'NOP', 'New York': 'NYK', 'Oklahoma': 'OKC',
			'Oklahoma City': 'OKC','Seattle':'OKC', 'Orlando':'ORL', 'Philadelphia':'PHI', 
			'Phoenix':'PHO','Portland':'POR', 'Sacramento':'SAC', 'San Antonio':'SAS', 
			'Toronto':'TOR','Utah':'UTA', 'Washington':'WAS', 'Baltimore':'WAS',
			'Atlanta Hawks': 'ATL', 'Boston Celtics':'BOS', 'Brooklyn Nets': 'BRK', 
			'New Jersey Nets':'BRK', 'Charlotte Hornets':'CHO','Chicago Bulls':'CHI', 
			'Cleveland Cavaliers':'CLE', 'Dallas Mavericks':'DAL', 
			'Denver Nuggets':'DEN', 'Detroit Pistons':'DET','Golden State Warriors':'GSW', 
			'Houston Rockets':'HOU', 'Indiana Pacers':'IND', 
			'L.A. Clippers': 'LAC','L.A. Lakers':'LAL', 'Memphis Grizzlies':'MEM', 
			'Miami Heat':'MIA', 'New Orleans Pelicans':'NOP', 
			'Milwaukee Bucks':'MIL', 'Minnesota Timberwolves':'MIN', 'Detroit':'DET',
			'New York Knicks': 'NYK', 'Oklahoma City Thunder': 'OKC','Orlando Magic':'ORL', 
			'Philadelphia 76ers':'PHI', 'Phoenix Suns':'PHO','Portland Trail Blazers':'POR', 
			'Sacramento Kings':'SAC', 'San Antonio':'SAS', 'Toronto Raptors':'TOR',
			'Utah Jazz':'UTA', 'Washington Wizards':'WAS', 'LA Clippers': 'LAC', 
			'LA Lakers':'LAL', 'Charlotte': 'CHO', 'NewYork':'NYK', 'SanAntonio':'SAS',
			'OklahomaCity':'OKC', 'LALakers': 'LAL', 'LAClippers':'LAC', 'NewOrleans':'NOP',
			'NewJersey':'BRK', 'GoldenState':'GSW'}


bet_type_id = {'ml':1, 'ps':2, 'total':3}

correct_team_id = {'PHX': 'PHO', 'BKN':'BRK', 'NOH': 'NOP', 'NJN':'BRK', 'CHA': 'CHO'}

bet_dict = {'PK':100, 'NL':-110}

def get_team_id(team):
	return team_id[team]

def get_home(team):
	return team_id[team.split('@')[1]]

def get_away(team):
	return team_id[team.split('@')[0]]

def get_odds(season, start_date, end_date, market_id):
	csv = f'csv/odds/{season}_{market_id}.csv'
	if not os.path.isdir('csv/odds'):
		os.makedirs('csv/odds')
	if os.path.isfile(csv):
		return
	nba = NBA()
	nba_market_ids= nba.market_ids(market_id)
	sb = Sportsbook()
	sb_ids = sb.ids(['pinnacle', 'bodog', '5dimes', 'sports interaction', 
					 'bet365','bovada','mybookie', 'gtbets', 'intertops', 'wagerweb'])

	days = (end_date - start_date).days
	weeks = days // 31 + 2
	lines_dataframes = []
	start_date = datetime(start_date.year, start_date.month, start_date.day)

	for i in tqdm(range(weeks), colour='green', position=0):
		tqdm.write(f'''Retrieving {market_id} lines for games in range [{start_date + timedelta((i-1)*31)}, {start_date + timedelta(i*31)}]''')
		time.sleep(1)
		e = EventsByDateRange(nba.league_id, 
			start_date + timedelta(days=(i-1)*31), 
			start_date + timedelta(days=i*31))
		cl = CurrentLines(e.ids(), nba_market_ids, sb_ids)
		lines_dataframes.append(cl.dataframe(e))

	combined = pd.concat(lines_dataframes)

	if not combined.empty:
		combined = combined.loc[combined['participant score'].notnull(),:]
		combined = combined.loc[combined['participant'].notnull(),:]
		combined.rename(columns={'participant': 'team_abbr',
								'decimal odds':'decimal_odds', 'spread / total': 'spread',
								'american odds':'vegas_odds'}, 
								inplace=True)
		combined['datetime'] = pd.to_datetime(combined["datetime"])

		combined['home_team'] = combined['event'].map(lambda x: x.split('@')[1])
		combined['away_team'] = combined['event'].map(lambda x: x.split('@')[0])

		combined['home_team_exists'] = combined['home_team'].map(lambda x: x in team_id)
		combined['away_team_exists'] = combined['away_team'].map(lambda x: x in team_id)								

		combined = combined.loc[combined['home_team_exists'] & combined['away_team_exists']]

		combined['home_abbr'] = combined['home_team'].map(get_team_id)
		combined['away_abbr'] = combined['away_team'].map(get_team_id)
		
		combined['bet_type_id'] = bet_type_id[market_id]
		combined['team_abbr'] = combined['team_abbr'].map(lambda x: 
				correct_team_id[x] if x in correct_team_id else x)

		combined['decimal_odds'] = combined['vegas_odds'].map(vegas_to_decimal)
		drop_columns = ['market id','event id', 'participant score', 'market','event', 
						'participant id', 'profit', 'result','home_team', 'away_team', 
						'sportsbook id', 'participant full name', 'home_team_exists',
						'away_team_exists', 'sportsbook alias']

		combined.drop([x for x in drop_columns if x in combined.columns], axis=1, inplace=True)
		combined.to_csv(csv,mode='w+', index=False, header=True)


def get_participant(event_id, is_home):
	time.sleep(1)
	e = EventsByEventIds(event_id).list()[0]
	for p in e['participants']:
		if p['is home'] == is_home:
			return p['source']['abbreviation']

def save_odds():
	conn = db_func.get_conn()
	cur = conn.cursor()
	query = ''' SELECT season, start_date, end_date 
				FROM season'''
	cur.execute(query)
	for season, start_date, end_date in tqdm(cur.fetchall(), colour='cyan', position=1):
		if season < 2007:
			continue
		get_odds(season, start_date, end_date, 'ml')
		get_odds(season, start_date, end_date, 'ps')
		get_odds(season, start_date, end_date, 'total')

	conn.close()

def insert_odds():
	conn = db_func.get_conn()
	conn.autocommit = True
	cur = conn.cursor()
	query = '''SELECT * 
			   FROM bet_type'''
	cur.execute(query)
	if len(cur.fetchall()) == 0:
		sif.insert_bet_type('db_src/bet_type.csv', cur)
		conn.commit()
	db_func.truncate_imports(cur)
	csvs = [f'csv/odds/{f}' for f in listdir('csv/odds') if isfile(join('csv/odds', f))]
	for csv in tqdm(csvs, colour='red', position=1):
		sif.insert_to_imports(csv)
	conn.commit()

	query = '''ANALYZE imports'''
	db_func.exec_query(conn, query)

	sif.imports_to_bets(cur)
	conn.commit()
	query = '''ANALYZE odds'''
	db_func.exec_query(conn, query)


	sif.imports_to_bets_total(cur)

	conn.commit()
	conn.close()

#Sportsbookreview is missing odds for certain matches (OKC-Seattle Supersonics and others)
#They will be filled in using moneyline odds data from sportsbookreviewonline.com
def fill_missing_odds():
	conn = db_func.get_conn()
	conn.autocommit = True
	cur = conn.cursor()
	db_func.truncate_imports(cur)

	if not os.path.isdir('csv/sbro_odds/modified'):
		os.makedirs('csv/sbro_odds/modified')

	csvs = [f'csv/sbro_odds/{f}' for f in listdir('csv/sbro_odds') 
				if isfile(join('csv/sbro_odds', f))]
	for csv in tqdm(csvs, colour='red', position=1):
		modify_sbro_odds(csv)
	modified_csvs = [f'csv/sbro_odds/modified/{f}' for f in listdir('csv/sbro_odds/modified') 
				if isfile(join('csv/sbro_odds/modified', f))]
	for csv in tqdm(modified_csvs, colour='red', position=1):
		sif.insert_to_imports(csv)
	conn.commit()
	sif.fill_missing_odds(cur)
	
	conn.commit()
	conn.close()

def modify_sbro_odds(csv):
	df = pd.read_csv(csv)
	season = int('20'+re.findall(r'-[0-9]{2}', csv)[0].lstrip('-'))
	df.rename(columns={'Date': 'datetime', 'ML':'vegas_odds',
					'Team':'team_abbr'}, inplace=True)
					
	if season != 2020:
		df['datetime'] = df['datetime'].map(lambda x: str(x) + str(season) if len(str(x)) == 3 else \
					str(x) + str(season-1))
	else:
		df['datetime'] = df.apply(lambda x: str(x['datetime'])+ str(season) if x['datetime']<= 1022 else \
						str(x['datetime']) + str(season-1), axis=1)

	df['datetime'] = df['datetime'].map(sbro_dates_to_date)
	df['team_abbr'] = df['team_abbr'].map(lambda x: team_id[x])
	df['sportsbook'] = 'sbro'
	df['bet_type_id'] = 1
	df['vegas_odds'] = df['vegas_odds'].map(lambda x: bet_dict[x] if x in bet_dict else int(x))
	df['decimal_odds'] = df['vegas_odds'].map(vegas_to_decimal)

	df = df[['datetime','team_abbr', 'vegas_odds', 'sportsbook', 
			'bet_type_id', 'decimal_odds']]
	df.to_csv(f'csv/sbro_odds/modified/{season}_sbro_odds.csv', mode='w+',index=False, header=True)

def sbro_dates_to_date(date):
	date = str(date)
	year = date[-4:]
	day = date[-6:-4]
	if len(date) == 8:
		month = date[0:2]
	else:
		month = date[0:1]
	return datetime(int(year),int(month),int(day))

def vegas_to_decimal(vegas_odds):
	vegas_odds =int(vegas_odds)
	if vegas_odds > 0:
		return (vegas_odds+100)/100
	else:
		vegas_odds = abs(vegas_odds)
		return (vegas_odds+100)/vegas_odds


def main():
	save_odds()
	insert_odds()
	fill_missing_odds()

if __name__ == '__main__':
	main()