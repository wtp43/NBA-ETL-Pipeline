import db_func
import db_config
import psycopg2
from psycopg2 import pool
from psycopg2 import OperationalError, errorcodes, errors, Error
import sys



insert_teams_query = \
	'''INSERT INTO tickers
		(ticker, exchange)
		SELECT im.ticker, im.ticker
		FROM imports as im
		WHERE NOT EXISTS
			   (SELECT *
				FROM tickers AS t, imports as im
				WHERE t.ticker = im.ticker
					AND t.exchange = im.exchange);'''


def main():
	try:
		conn = psycopg2.connect(user = db_config.user,
									password = db_config.password,
									host = db_config.host,
									port = db_config.port,
									database = db_config.database)
		if(conn):
			print("Connection created successfully")
		

		team_list = 'db_src/NBA_Teams.csv'
		cur = conn.cursor()
		headers = ['symbol', 'name']
		with open(team_list, 'r') as f: 
			next(f)
			cur.copy_from(f, 'team', columns=headers,sep=',')

		conn.commit()
	except Exception as err:
		conn = None
		print(err)
	finally:
		if (conn):
			conn.close()
			print("PostgreSQL connection is closed")


if __name__ == '__main__':
	main()
