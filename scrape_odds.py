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

team_id = {'Atlanta': 1, 'Boston':2, 'Brooklyn': 3, 'New Jersey':3, 'Charlotte':4,
			'Chicago':5, 'Cleveland':6, 'Dallas':7, 'Denver':8, 'Detroit':9,
			'Golden State':10, 'Houston':11, 'Indiana':12, 'L.A. Clippers': 13,
			'L.A. Lakers':14, 'Memphis':15, 'Miami':16, 'Milwaukee':17, 
			'Minnesota':18, 'New Orleans':19, 'New York': 20, 'Oklahoma': 21,
			'Oklahoma City': 21,'Seattle':21, 'Orlando':22, 'Philadelphia':23, 
			'Phoenix':24,'Portland':25, 'Sacramento':26, 'San Antonio':27, 
			'Toronto':28,'Utah':29, 'Washington':30, 'Baltimore':30,
			'Atlanta Hawks': 1, 'Boston Celtics':2, 'Brooklyn Nets': 3, 
			'New Jersey Nets':3, 'Charlotte Hornets':4,'Chicago Bulls':5, 
			'Cleveland Cavaliers':6, 'Dallas Mavericks':7, 
			'Denver Nuggets':8, 'Detroit Pistons':9,'Golden State Warrios':10, 
			'Houston Rockets':11, 'Indiana Pacers':12, 
			'L.A. Clippers': 13,'L.A. Lakers':14, 'Memphis Grizzlies':15, 'Miami Heat':16, 
			'Milwaukee Bucks':17, 'Minnesota Timberwolves':18, 'New Orleans Pelicans':19, 
			'New York Knicks': 20, 'Oklahoma City Thunder': 21,'Orlando Magic':22, 
			'Philadelphia 76ers':23, 'Phoenix Suns':24,'Portland Trail Blazers':25, 
			'Sacramento Kings':26, 'San Antonio':27, 'Toronto Raptors':28,
			'Utah Jazz':29, 'Washington Wizards':30, 'LA Clippers': 13, 'LA Lakers':14}

bet_type_id = {'ml':1, 'ps':2, 'total':3}

def get_team_id(team):
	return team_id[team]

def get_home(team):
	return team_id[team.split('@')[1]]

def get_away(team):
	return team_id[team.split('@')[0]]

def get_odds(season, start_date, end_date, market_id):
	csv = f'odds/{season}_{market_id}.csv'
	if not os.path.isdir('odds'):
		os.makedirs('odds')
	if os.path.isfile(csv):
		return
	nba = NBA()
	nba_market_ids= nba.market_ids(market_id)
	sb = Sportsbook()
	sb_ids = sb.ids(['pinnacle', 'bet365'])
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
		combined.rename(columns={'participant': 'team_abbr', 'datetime':'date',
								'decimal odds':'decimal_odds', 'spread / total': 'spread',
								'american odds':'vegas_odds'}, 
								inplace=True)
		combined['date'] = pd.to_datetime(combined["date"])
		combined['date'] = combined['date'].map(lambda x: x.date())

		combined['home_team'] = combined['event'].map(lambda x: x.split('@')[1])
		combined['away_team'] = combined['event'].map(lambda x: x.split('@')[0])

		combined['home_team_exists'] = combined['home_team'].map(lambda x: x in team_id)
		combined['away_team_exists'] = combined['away_team'].map(lambda x: x in team_id)								

		combined = combined.loc[combined['home_team_exists'] & combined['away_team_exists']]

		combined['home_id'] = combined['home_team'].map(get_team_id)
		combined['away_id'] = combined['away_team'].map(get_team_id)

		combined['home_id'] = combined['event'].map(get_home)
		combined['away_id'] = combined['event'].map(get_away)
		
		combined['bet_type_id'] = bet_type_id[market_id]

		drop_columns = ['market id','event id', 'participant score', 'market','event', 
						'participant id', 'profit', 'result','home_team', 'away_team', 
						'sportsbook id', 'participant full name', 'home_team_exists',
						'away_team_exists']
		combined.drop(columns=drop_columns, axis=1, inplace=True)
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
		get_odds(season, start_date, end_date, 'ps')
		get_odds(season, start_date, end_date, 'ml')
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
	csvs = [f'odds/{f}' for f in listdir('odds') if isfile(join('odds', f))]
	for csv in tqdm(csvs, colour='magenta', position=1):
		sif.insert_to_imports(csv)

	conn.close()


def main():
	save_odds()
	insert_odds()
	
if __name__ == '__main__':
	main()