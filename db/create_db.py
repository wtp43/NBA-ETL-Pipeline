import db_func as db_func
import db_schema as db_schema
from dotenv import load_dotenv
from pathlib import Path


def main():
	try:
		basepath = Path()
		# Load the environment variables
		envars = basepath.cwd() / 'db.env'
		load_dotenv(envars)
		conn = db_func.get_conn()
		if(conn):
			print("Connection created successfully")

		db_func.exec_query(conn, db_schema.create_season_table)
		db_func.exec_query(conn, db_schema.create_team_table)
		db_func.exec_query(conn, db_schema.create_team_name_table)
		db_func.exec_query(conn, db_schema.create_arena_table)

		db_func.exec_query(conn, db_schema.create_player_table)
		db_func.exec_query(conn, db_schema.create_player_team_table)
		db_func.exec_query(conn, db_schema.create_match_table)
		db_func.exec_query(conn, db_schema.create_injury_table)

		db_func.exec_query(conn, db_schema.create_imports_table)
		db_func.exec_query(conn, db_schema.create_player_performance_table)

		db_func.exec_query(conn, db_schema.create_bet_type_table)
		db_func.exec_query(conn, db_schema.create_odds_table)
	
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
