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

DEBUG = False
pd.options.display.max_columns = None
pd.options.display.max_rows = None

team_abbr = {'Atlanta Hawks':'ATL', 'Boston Celtics' : 'BOS', 'Brooklyn Nets': 'NJN',
			'Charlotte Hornets': 'CHA', 'Chicago Bulls':'CHI','Cleveland Cavaliers':'CLE',
			'Dallas Mavericks':'DAL','Denver Nuggets':'DEN', 'Detroit Pistons':'DET',
			'Golden State Warriors':'GSW','Houston Rockets':'HOU','Indiana Pacers':'IND',
			'Los Angeles Clippers':'LAC','Los Angeles Lakers':'LAL','Memphis Grizzlies':'MEM',
			'Miami Heat':'MIA','Milwaukee Bucks':'MIL','Minnesota Timberwolves':'MIN',
			'New Orleans Pelicans':'NOH','New York Knicks':'NYK','Oklahoma City Thunder':'OKC',
			'Orlando Magic':'ORL','Philadelphia 76ers':'PHI','Phoenix Suns':'PHO',
			'Portland Trail Blazers':'POR','Sacramento Kings':'SAC','San Antonio Spurs':'SAS',
			'Toronto Raptors':'TOR','Utah Jazz':'UTA','Washington Wizards':'WAS'}



def save_match_html(season):
	try:
		url = "https://www.basketball-reference.com/leagues/NBA_" + season+ "_games.html"
		exception = True
		print(url)
		while(exception):
			try:
				response = requests.request("GET", url)
				soup = BeautifulSoup(response.content, 'html.parser')
				exception = False
			except requests.exceptions.RequestException as e:
				print(e)
				time.sleep(20)
				exception = True
		if not os.path.isdir("bs4_html"):
			os.makedirs("bs4_html")
		filename = os.path.join(os.getcwd(),"bs4_html/match_list/" \
			+ "/" +season+".html")
		os.makedirs(os.path.dirname(filename), exist_ok=True)
		with open(filename, "w", encoding='utf-8') as f:
			f.write(str(soup))
	except Exception as err:
		print(err)


def save_boxscore_html(team, date):
	try:
		url = "https://www.basketball-reference.com/boxscores/" + date + team + ".html"
		exception = True
	
		while(exception):
			try:
				response = requests.request("GET", url)
				soup = BeautifulSoup(response.content, 'html.parser')
				exception = False
			except requests.exceptions.RequestException as e:
				print(e)
				time.sleep(20)
				exception = True
		if not os.path.isdir("bs4_html"):
			os.makedirs("bs4_html")
		filename = os.path.join(os.getcwd(),"bs4_html/boxscores/" \
			+ "/" +date+team+".html")
		os.makedirs(os.path.dirname(filename), exist_ok=True)
		with open(filename, "w", encoding='utf-8') as f:
			f.write(str(soup))
	except Exception as err:
		print(err)

def save_all_player_performances(season):
	file_path = 'csv/' + season + '/match_list.csv'
	df = pd.read_csv(file_path)
	for index, row in df.iterrows():
		print(row['date'], row['home'])

def save_match_data(html_path):
	try:
		with open(html_path, 'r', encoding="utf8") as f:
			contents = f.read()
			soup = BeautifulSoup(contents, 'lxml')

		match_urls = soup.find_all(\
			href=re.compile('\/leagues\/NBA_[0-9]{4}_games-[A-Za-z]{1,}.html'))
		match_urls = [re.findall('\/leagues\/NBA_[0-9]{4}_games-[A-Za-z]{1,}.html',\
			 str(match_urls[i]))[0] \
			for i in range(len(match_urls))]
		season = re.findall('[0-9]{4}', html_path)[0]
		directory = "csv/" + season
		if not os.path.isdir(directory):
			os.makedirs(directory)
		file_path = directory+"/"+"match_list.csv"
		if os.path.exists(file_path):
			os.remove(file_path)
		for i in range(len(match_urls)):
			url = 'http://basketball-reference.com' + match_urls[i]
			exception = True
			while(exception):
				try:
					response = requests.request("GET", url)
					soup = BeautifulSoup(response.content, 'html.parser')
					exception = False
				except requests.exceptions.RequestException as e:
					print(e)
					time.sleep(20)
					exception = True
			table = soup.findAll('table', attrs={'id': 'schedule'})
			df = pd.read_html(str(table), flavor='bs4', header=[0])[0]
			df.drop(columns=df.columns[[1,6,7,8,9]], inplace=True)
			df.rename(columns={'Date':'date', 'Visitor/Neutral': 'away', 
				'Home/Neutral': 'home', 'PTS': 'away_pts', 'PTS.1': 'home_pts'}, inplace=True)

			df['date'] = pd.to_datetime(df.date)
			df = df[df['away_pts'].notna()]
			df['date'] = df['date'].dt.strftime('%Y%m%d')
			df['away'] = df['away'].apply(lambda x: team_abbr[x])
			df['home'] = df['home'].apply(lambda x: team_abbr[x])
			
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

def save_player_data(html_path, match_id):
	try:
		with open(html_path, 'r', encoding="utf8") as f:
			contents = f.read()
			soup = BeautifulSoup(contents, 'lxml')

		line_score_table = soup.findAll(string=re.compile("div_line_score"))
		teams = re.findall("teams\/[A-Z]{3}\/[0-9]{4}", str(line_score_table))

		season = teams[0][len(teams[0])-4:]
		teams = [teams[i][0:len(teams[0])-5].lstrip('teams/') for i in range(len(teams))]

		linescores = re.findall('data-stat="[0-9]" >[0-9]{1,}<', str(line_score_table))
		linescores = [linescores[i][len(linescores[i])-3:-1] for i in range(len(linescores))]


		directory = "csv/" + season
		if not os.path.isdir(directory):
			os.makedirs(directory)
		file_path = directory+"/"+re.findall("[0-9]{8}[A-Z]{3}", html_path)[0] + ".csv"
		if os.path.exists(file_path):
			os.remove(file_path)

		table = soup.findAll('table')
		for i in range(len(teams)):
			basic_stat_tag = 'box-' + teams[i] + '-game-basic'
			advanced_stat_tag = 'box-' + teams[i] + '-game-advanced'

			basic_df = pd.read_html(str(table), flavor='bs4', 
				header=[1], attrs= {'id': basic_stat_tag})[0]

			df = pd.read_html(str(table), flavor='bs4', 
				header=[1], attrs= {'id': advanced_stat_tag})[0]

			df['team_name'] = teams[i]
			df['starter'] = 1
			df['match_id'] = match_id
			df['date'] = re.findall("[0-9]{8}", html_path)[0]
			starter = True
			drop_rows = []
			for j in range(len(df)):
				
				if df.iloc[j,0] == 'Reserves':
					starter = False
					drop_rows.append(j)
				if df.iloc[j,0] == 'Team Totals':
					drop_rows.append(j)
				if df.iloc[j,1] == 'Did Not Play':
					drop_rows.append(j)
				else:
					df.iloc[j, -1] = [int(starter)]

			df.drop(drop_rows, inplace=True)
			basic_df.drop(drop_rows, inplace=True)
			basic_df.drop(['MP', 'Starters'], axis=1, inplace=True)

			df['MP'] = df['MP'].apply(seconder)
		
			df = pd.concat([df, basic_df], axis = 1)
			df.rename(columns={'Starters': 'player_name', 'MP': 'sp',
			'TS%': 'ts_p', 'eFG%': 'efg_p', '3PAr':'3par', 'FTr': 'ftr',
			'ORB%': 'orb_p', 'DRB%':'drb_p', 'TRB%': 'trb_p', 'AST%':'ast_p',
			'STL%': 'stl_p', 'BLK%':'blk_p', 'TOV%': 'tov_p', 'USG%': 'usg_p',
			'ORtg': 'ortg', 'DRtg':'drtg', 'BPM':'bpm', 'FG': 'fg',
			'FGA': 'fga', 'FG%': 'fg_p', '3P': '3p', '3PA': '3pa', '3P%': '3p_p',
			'FT': 'ft', 'FTA': 'fta', 'FT%': 'ft_p', 'ORB': 'orb',
			'DRB': 'drb', 'TRB': 'trb', 'AST': 'ast','STL': 'stl',
			'BLK': 'blk', 'TOV': 'tov', 'PF': 'pf', 'PTS': 'pts', '+/-': 'pm'}, inplace=True)
			print(df.columns)
			if i ==0:
				df.to_csv(file_path, mode='a',index=False, header=True)
			else:
				df.to_csv(file_path, mode='a',index=False, header=False)

	except Exception as err:
		raise err
		

