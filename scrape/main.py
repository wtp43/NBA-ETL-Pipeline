from datetime import datetime
import sys
import bbref_scraper as sc

def main():
	args = sys.argv[1:]
	date_range = []
	# if args[1] == '-u':
	# 	today = datetime.today().strftime('%Y-%m-%d')
	# 	yesterday = today - datetime.timedelta(days=1)
	# 	date_range = [yesterday, today]
	sc.scrape(*date_range)
		
if __name__ == "__main__":
    main()