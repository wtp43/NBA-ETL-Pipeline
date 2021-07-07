import pprint
from datetime import datetime
import pandas as pd
pd.options.display.max_columns = None
pd.options.display.max_rows = None
import json
from pysbr import *

def scrape_odds():
	nba = NBA()
	start_date = datetime.strptime('20061105', '%Y%m%d')
	end_date = datetime.strptime('20061106', '%Y%m%d')

	sb = Sportsbook()
	lm = LeagueMarkets(nba.league_id)
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
	e = EventsByDateRange(nba.league_id, start_date, end_date)
	# for i in range(len(e.list())):
	# 	q = e.list()[i]['participants']
	# 	teams.extend([(k['source']['abbreviation'], k['participant id']) for k in q])
	json.dump(e.list(), open( "odds/matches.json", 'w' ))
	#data = json.load( open( "file_name.json" ) )
	i = 0
	participants = [j['participant id'] for j in e.list()[i]['participants']]
	for i in range(len(e.list())):
		event_id = e.list()[i]['event id']
		#print(event_id)
		eventmarkets = EventMarkets(24221)
		#print(eventmarkets.list())


	#print(participants)

	#print(e.dataframe())
	# #pprint.pprint(json_dict)
	# print(set(teams))

	
	#print(Current(e.list()[0]))
	market_ids = LeagueMarkets(nba.league_id).list()
	market_ids = [d['market id'] for d in market_ids]
	sb_ids = sb.ids(['pinnacle', 'bodog'])
	print(market_ids)

	for j in range(len(market_ids)):
		lh = LineHistory(e.list()[0]['event id'], market_ids[j], sb_ids[0], participants)
		print(lh.list())
	#print(BestLines(24221, market_ids).list())


	return

# def save_obj(obj, name ):
#     with open('obj/'+ name + '.pkl', 'wb') as f:
#         pickle.dump(obj, f, pickle.HIGHEST_PROTOCOL)

# def load_obj(name ):
#     with open('obj/' + name + '.pkl', 'rb') as f:
#         return pickle.load(f)

def main():
	scrape_odds()

if __name__ == '__main__':
	main()