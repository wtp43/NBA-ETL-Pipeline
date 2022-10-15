import psycopg2
from psycopg2 import Error, pool
import sys
import os
from dotenv import load_dotenv
from pathlib import Path

basepath = Path()
# Load the environment variables
envars = basepath.cwd().parent.joinpath('db/db.env')
load_dotenv(envars)

# threaded_postgreSQL_pool = psycopg2.pool.ThreadedConnectionPool(1, 20,
# 									user = os.getenv('DBUSER'),
# 									password = os.getenv('PASSWORD'),
# 									host = os.getenv('HOST'),
# 									port = os.getenv('PORT'),
# 									database = os.getenv('DATABASE'), 
# 									sslmode='require')
#threaded_postgreSQL_pool = []
def truncate_imports(cur):
	try:
		cur.execute('TRUNCATE TABLE imports;')
	except Exception as err:
		raise err

def get_conn():
	try:
		basepath = Path()
		# Load the environment variables
		envars = basepath.cwd().parent.joinpath('db/db.env')
		load_dotenv(envars)
		#create connection and cursor
		conn = psycopg2.connect(user = os.getenv('DBUSER'),
									password = os.getenv('PASSWORD'),
									host = os.getenv('HOST'),
									port = os.getenv('PORT'),
									database = os.getenv('DATABASE')) 
									#sslmode='require')
		#Get the base directory
		#conn = psycopg2.connect(os.getenv('DATABASE_URL'), sslmode='require')
		# conn = psycopg2.connect(database_url, sslmode='require')
		#conn = threaded_postgreSQL_pool.getconn()
		#conn.autocommit = True

	except Exception as err:
		raise err
	return conn


def print_psycopg2_exception(err):
    # get details about the exception
    err_type, traceback = sys.exc_info()
   # err_type = sys.exc_info()

    # get the line number when exception occured
    line_num = traceback.tb_lineno

    # print the connect() error
    print ("\npsycopg2 ERROR:", err, "on line number:", line_num)
    print ("psycopg2 traceback:", traceback, "-- type:", err_type)

    # psycopg2 extensions.Diagnostics object attribute
    print ("\nextensions.Diagnostics:", err.diag)

    # print the pgcode and pgerror exceptions
    print ("pgerror:", err.pgerror)
    print ("pgcode:", err.pgcode, "\n")

def exec_query(conn, query):
	try:
		cursor = conn.cursor()
		cursor.execute(query)
	except Error as err:
		conn.rollback()
		cursor.close()
		raise err
	finally:
		cursor.close()








