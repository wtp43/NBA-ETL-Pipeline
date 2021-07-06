import pysbr as py
import pprint
from datetime import datetime
import pandas as pd
pd.options.display.max_columns = None
pd.options.display.max_rows = None

def scrape_odds():
	nba = py.NBA()
	start_date = datetime.strptime('20061105', '%Y%m%d')
	end_date = datetime.strptime('20061106', '%Y%m%d')

	sb = py.Sportsbook()
	lm = py.LeagueMarkets(nba.league_id)
	#print(lm.list())

	#missing odds
	#Insert first allowing nulls in database
	#skip these matches in strategy backtest

	# nba_league = py.LeagueHierarchy(nba.league_id)
	# json_dict = nba_league.list()
	# formatted_json_str = pprint.pformat(json_dict)
	# pprint.pprint(json_dict)
	#print(nba_league.list())
	# teams = []
	e = py.EventsByDateRange(nba.league_id, start_date, end_date)
	# for i in range(len(e.list())):
	# 	q = e.list()[i]['participants']
	# 	teams.extend([(k['source']['abbreviation'], k['participant id']) for k in q])
	print(e.dataframe())
	lines = py.LineHistory()
	# #pprint.pprint(json_dict)
	# print(set(teams))
	return

def main():
	scrape_odds()

if __name__ == '__main__':
	main()