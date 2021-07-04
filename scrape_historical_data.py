from pandas.core.frame import DataFrame
import requests
from bs4 import BeautifulSoup
import re
from datetime import date, datetime, timedelta
import os
import errno
import numpy
import pandas as pd
import time
import db_func
import logging

DEBUG = False
pd.options.display.max_columns = None
pd.options.display.max_rows = None

def get_team_abbr():
	conn = db_func.get_conn()
	cur = conn.cursor()
	
	query = \
		'''SELECT t.team_name, t.team_id
			FROM team as t;'''
	cur.execute(query)
	
	team_abbr = {}
	for key, val in cur.fetchall():
		team_abbr[key] = val
	conn.close()
	
	return team_abbr

def player_data_to_csv(html, bbref_endpoint, player_name):
	"""
	player_data_to_csv saves relevant player infro from bbref_endpoint 
		html to csv
	Args:
		:param html: html of bbref player page
			ex: /players/b/bibbymi01.html

	:side effect: saves player info from bbref
    :return: None
    """
	try:
		file_path = 'csv' + bbref_endpoint[:-5] + '.csv'
		if os.path.isfile(file_path):
			return 0
		with open(html, 'r', encoding="utf8") as f:
			contents = f.read()
			soup = BeautifulSoup(contents, 'lxml')
		table = soup.find_all('table', attrs={'id': 'per_game'})
		if not table:
			return 1
		df = pd.read_html(str(table), flavor='bs4', header=[0])[0]
		df.columns = df.columns.str.lower()
		df.columns = [s.replace('%', '_pct') for s in df.columns]
		df.columns = [s.replace('3', 'three') for s in df.columns]
		df.columns = [s.replace('2', 'two') for s in df.columns]

		if DEBUG:
			print(df)
			print(df.columns)

		cutoff = df.index[df['season'] == 'Career']
		keep_elements = [i for i in range(cutoff[0])]
		df.drop(df.index.difference(keep_elements), axis=0, inplace=True)
		df['season'] = df['season'].apply(lambda x: x[0:2] + x[-2:])
		df['pos'] = df['pos'].apply(lambda x: x.replace(',', ':'))
		df['bbref_endpoint'] = bbref_endpoint
		df['player_name'] = player_name
		df.rename(columns={'team': 'team_id', 'tm':'team_id'}, inplace=True)

		#Drop rows where player did not play(invalid team_id = Did not Play)
		df = df[df['team_id'].map(len)==3]
		df = df[df['team_id'] != 'TOT']
		df.fillna(0, inplace=True)
		df.replace('', 0, inplace=True)
		
		directory = 'csv' + re.findall('/players/[a-z]{1}',bbref_endpoint)[0]
		
		if not os.path.isdir(directory):
			os.makedirs(directory)
		
		file_path = 'csv' + bbref_endpoint[:-5] + '.csv'
		df.to_csv(file_path, mode='w+',index=False, header=True)
		return 0
	except Exception as err:
		raise err

def get_endpoints_df(html):
	try:
		with open(html, 'r', encoding="utf8") as f:
			contents = f.read()
			soup = BeautifulSoup(contents, 'lxml')

		roster_table = soup.find('table', attrs={'id': 'roster'})
		names = []
		endpoints=[]  

		trs = roster_table.find_all('tr')
		for i in range(1,len(trs)):
			#Only add players that are playing/active
			#(active = has player/jersey number)
			if trs[i].th.text != '':
				names.append(trs[i].a.text)
				endpoints.append(trs[i].a['href'])

		print(names)
		df = DataFrame(list(zip(endpoints, names)))
		return df
	except Exception as err:
		print(err)

	
def save_html(url, file):
	"""
    save_html saves the html page for a given match (boxscores)
	
	Args: 
		param team: home team of match
		param date: date of match
	
	:side effect: saves the respective html page in file
    :return: None
    """
	if os.path.isfile(file) and os.stat(file).st_size !=0:
		logging.info(file +": has already been scraped before")
		return
	try:
		exception = True
		while(exception):
			try:
				response = requests.request("GET", url)
				soup = BeautifulSoup(response.content, 'html.parser')
				exception = False
			except requests.exceptions.RequestException as e:
				print(e)
				exception = True
		if not os.path.isdir("bs4_html"):
			os.makedirs("bs4_html")
		os.makedirs(os.path.dirname(file), exist_ok=True)
		with open(file, "w", encoding='utf-8') as f:
			f.write(str(soup))
	except Exception as err:
		print(err)


def match_list_to_csv(match_list_html):
	"""
    matches_to_csv saves the html page for a given match (boxscores)

	Args: 
		:param match_list_html: html of match lists 
			bs4_html/match_list/year.html
	
	:side effect: csv with all matches in given season/year
    :return: None
    """
	try:
		with open(match_list_html, 'r', encoding="utf8") as f:
			contents = f.read()
			soup = BeautifulSoup(contents, 'lxml')

		match_urls = soup.find_all(\
			href=re.compile(r'\/leagues\/NBA_[0-9]{4}_games-[A-Za-z]{1,}.html'))
		match_urls = [re.findall(r'\/leagues\/NBA_[0-9]{4}_games-[A-Za-z]{1,}.html',\
			 str(match_urls[i]))[0] for i in range(len(match_urls))]
		season = re.findall('[0-9]{4}', match_list_html)[0]
		directory = "csv/" + season + '/'
		if not os.path.isdir(directory):
			os.makedirs(directory)
		file_path = directory+"/match_list.csv"
		if os.path.exists(file_path):
			os.remove(file_path)

		team_abbr = get_team_abbr()
		regular_game = True
		playoff_index = 0

		for i in range(len(match_urls)):
			url = 'http://basketball-reference.com' + match_urls[i]
			html = 'bs4_html/' + match_urls[i]
			save_html(url, html)
			with open(html, 'r', encoding="utf8") as f:
				contents = f.read()
				soup = BeautifulSoup(contents, 'lxml')
			table = soup.find_all('table', attrs={'id': 'schedule'})

			if regular_game:
				playoff_row = soup.find_all('playoff')
				playoff = soup.find('th', string='Playoffs')
				if playoff:
					playoff_row = playoff.find_parent('tr')
					playoff_index = len(playoff_row.find_previous_siblings('tr'))

			df = pd.read_html(str(table), flavor='bs4', header=[0])[0]
			
			playoff_index = df[df['Date']=='Playoffs'].index
			df['playoff_game'] = 0
			if not regular_game:
				df['playoff_game'] = 1
			elif len(playoff_index):
				df = df[df.index != playoff_index[0]]
				df.loc[playoff_index[0]:, 'playoff_game'] = 1
				regular_game = False

			df.drop(columns=df.columns[[1,6,7,8,9]], inplace=True)
			df.rename(columns={'Date':'date', 'Visitor/Neutral': 'away_id', 
				'Home/Neutral': 'home_id', 'PTS': 'away_pts', 'PTS.1': 'home_pts'}, inplace=True)

			df['date'] = pd.to_datetime(df.date)
			df = df[df['away_pts'].notna()]
			df['date'] = df['date'].dt.strftime('%Y%m%d')
			df['away_id'] = df['away_id'].apply(lambda x: team_abbr[x])
			df['home_id'] = df['home_id'].apply(lambda x: team_abbr[x])
			
			if i == 0:
				df.to_csv(file_path, mode='a',index=False, header=True)
			else:
				df.to_csv(file_path, mode='a',index=False, header=False)
	except Exception as err:
		raise err

def seconder(x):
    mins, secs = map(float, x.split(':'))
    td = timedelta(minutes=mins, seconds=secs)
    return td.total_seconds()

def boxscore_to_csv(match_html):
	'''
    match_data_to_csv saves the stats of every player in the match into a csv

	Args: 
		:param match_html: html of the given match 
			bs4_html/boxscores/date+hometeam.html
	
	:side effect: csv with match stats
    :return: None
    '''
	try:

		with open(match_html, 'r', encoding="utf8") as f:
			contents = f.read()
			soup = BeautifulSoup(contents, 'lxml')

		line_score_table = soup.find(string=re.compile("div_line_score"))
		teams = re.findall(r"teams\/[A-Z]{3}\/[0-9]{4}", str(line_score_table))
		#season = teams[0][len(teams[0])-4:]
		teams = [teams[i][0:len(teams[0])-5].lstrip('teams/') for i in range(len(teams))]
		linescores = re.findall('data-stat="[0-9]" >[0-9]{1,}<', str(line_score_table))
		linescores = [linescores[i][len(linescores[i])-3:-1] for i in range(len(linescores))]
		
		directory = "csv/boxscores"
		if not os.path.isdir(directory):
			os.makedirs(directory)
		file_path = directory+"/"+re.findall("[0-9]{9}[A-Z]{3}", match_html)[0] + ".csv"
		# if os.path.isfile(file_path) and os.stat(file_path).st_size !=0:
		# 	logging.info(file_path +": has already been inserted to a csv")
		# 	return
		with open(file_path, 'w', encoding="utf8") as f:
			f.truncate()
		
		table = soup.find_all('table')
		for i in range(len(teams)):
			basic_stat_tag = 'box-' + teams[i] + '-game-basic'
			advanced_stat_tag = 'box-' + teams[i] + '-game-advanced'

			basic_df = pd.read_html(str(table), flavor='bs4', 
				header=[1], attrs= {'id': basic_stat_tag})[0]

			df = pd.read_html(str(table), flavor='bs4', 
				header=[1], attrs= {'id': advanced_stat_tag})[0]

			df['team_id'] = teams[i]
			df['starter'] = 1
			df['inactive'] = 0
			df['date'] = re.findall("[0-9]{8}", match_html)[0]
			starter = True
			drop_rows = []

			df.loc[(df['MP']=='Did Not Play') | 
				(df['MP'] == 'Did Not Dress') |
				(df['MP'] == 'Player Suspended') |
				(df['MP'] == 'Not With Team'), 'MP'] = '0:00'

			for j in range(len(df)):
				if df.iloc[j,0] == 'Reserves':
					starter = False
					drop_rows.append(j)
				elif df.iloc[j,0] == 'Team Totals':
					drop_rows.append(j)
				else:
					df.loc[j,'starter'] = int(starter)
			df.drop(drop_rows, inplace=True)
			basic_df.drop(drop_rows, inplace=True)
			basic_df.drop(['MP', 'Starters'], axis=1, inplace=True)
			
			df['MP'] = df['MP'].apply(seconder)

			df = pd.concat([df, basic_df], axis = 1)
			df.rename(columns={'Starters': 'player_name', 'MP': 'sp',
				'+/-': 'pm'}, inplace=True)
			
			df.columns = df.columns.str.lower()
			df.columns = [s.replace('%', '_pct') for s in df.columns]
			df.columns = [s.replace('3', 'three') for s in df.columns]
			df.columns = [s.replace('2', 'two') for s in df.columns]
			df.replace({'Did Not Play': 0, 
						'Did Not Dress': 0,
						'Not With Team': 0,
						'Player Suspended': 0}, inplace=True)
			df.fillna(0, inplace=True)
			if i == 0:
				df.to_csv(file_path, mode='a',index=False, header=True)
			else:
				# Add injured/inactive players to last (2nd) iteration of dataframe
				inactive = re.findall("Inactive:.*div", str(soup))
				inactive = re.findall('>[^"/<>]+[A-z]+<', inactive[0])
				inactive = [s.rstrip('<').lstrip('>') for s in inactive]
				match_date = df.loc[0, 'date']
				for i in range(len(inactive)):
					if inactive[i] == teams[0] or inactive[i] == teams[1]:
						team = inactive[i]
					else:
						row = [0]*len(df.iloc[0])
						df.loc[df.index[-1]+1] = row
						df.loc[df.index[-1],'player_name'] = inactive[i]
						df.loc[df.index[-1],'team_id'] = team
						df.loc[df.index[-1],'date'] = match_date
						df.loc[df.index[-1], 'inactive'] = 1
				df.to_csv(file_path, mode='a',index=False, header=False)

	except Exception as err:
		logging.error(match_html+" boxscore processing failed")
		raise err


