import requests
from bs4 import BeautifulSoup
import re
from datetime import date, datetime
import os
import errno
import numpy
import pandas as pd
import time
import db_setup

DEBUG = False

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


def save_match_data(html_path):
	try:
		with open(html_path, 'r', encoding="utf8") as f:
			contents = f.read()
			soup = BeautifulSoup(contents, 'lxml')

		match_urls = soup.find_all(href=re.compile('\/leagues\/NBA_[0-9]{4}_games-[A-Za-z]{1,}.html'))
		match_urls = [re.findall('\/leagues\/NBA_[0-9]{4}_games-[A-Za-z]{1,}.html', str(match_urls[i]))[0] \
			for i in range(len(match_urls))]
		season = re.findall('[0-9]{4}', html_path)[0]
		directory = "csv/" + season
		if not os.path.isdir(directory):
			os.makedirs(directory)
		file_path = directory+"/"+season+ "_match_list.csv"
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
			pd.options.display.max_columns = None
			df.rename(columns={'Date':'date', 'Visitor/Neutral': 'away', 'Home/Neutral': 'home', 
			'PTS': 'away_pts', 'PTS.1': 'home_pts'}, inplace=True)
			df['date'] = pd.to_datetime(df.date)
			df['date'] = df['date'].dt.strftime('%Y%m%d')
			if i == 0:
				df.to_csv(file_path, mode='a',index=False, header=True)
			else:
				df.to_csv(file_path, mode='a',index=False, header=False)
	except Exception as err:
		raise err


def save_player_data(html_path, match_id):
	try:
	
		with open(html_path, 'r', encoding="utf8") as f:
			contents = f.read()
			soup = BeautifulSoup(contents, 'html5lib')

			
		line_score_table = soup.findAll(string=re.compile("div_line_score"))
		teams = re.findall("teams\/[A-Z]{3}\/[0-9]{4}", str(line_score_table))

		season = teams[0][len(teams[0])-4:]
		teams = [teams[i][0:len(teams[0])-5].lstrip('teams/') for i in range(len(teams))]


		linescores = re.findall('data-stat="[0-9]" >[0-9]{1,}<', str(line_score_table))
		linescores = [linescores[i][len(linescores[i])-3:-1] for i in range(len(linescores))]

		tag_id = []

		directory = "csv/" + season
		if not os.path.isdir(directory):
			os.makedirs(directory)
		file_path = directory+"/"+re.findall("[0-9]{8}[A-Z]{3}", html_path)[0] + ".csv"
		if os.path.exists(file_path):
			os.remove(file_path)

		table = soup.findAll('table')
		for i in range(len(teams)):
			tag_id.append('box-' + teams[i] + '-game-advanced')

			df = pd.read_html(str(table), flavor='bs4', header=[1], attrs= {'id': tag_id[i]})[0]
			df['team_name'] = teams[i]
			df['match_id'] = match_id
			df['starter'] = 1

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
			df.rename(columns={'Starters': 'name'}, inplace=True)
			df.drop(drop_rows, inplace=True)
			if i ==0:
				df.to_csv(file_path, mode='a',index=False, header=True)
			else:
				df.to_csv(file_path, mode='a',index=False, header=False)

	except Exception as err:
		raise err
		

