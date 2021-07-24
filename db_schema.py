# Section 1: Match info
# Section 2: Player info
create_imports_table = \
	'''CREATE UNLOGGED TABLE IF NOT EXISTS imports(
		date			DATE,
		away_pts		REAL,
		home_pts		REAL,
		away_abbr 		TEXT,
		home_abbr 		TEXT,
		elevation		REAL,
		playoff_game	REAL,

		player_name		TEXT,
		bbref_endpoint 	TEXT,
		season			REAL,
		age				REAL,
		team_abbr		TEXT,
		lg				TEXT,
		pos				TEXT,
		g				REAL,
		gs				REAL,
		mp				REAL DEFAULT 0,
		fg				REAL DEFAULT 0,
		fga				REAL DEFAULT 0,
		fg_pct			REAL DEFAULT 0,
		threep			REAL DEFAULT 0,
		threepa			REAL DEFAULT 0,
		threep_pct		REAL DEFAULT 0,
		twop			REAL DEFAULT 0,
		twopa			REAL DEFAULT 0,
		twop_pct		REAL DEFAULT 0, 
		efg_pct			REAL DEFAULT 0,
		ft				REAL DEFAULT 0,
		fta				REAL DEFAULT 0,
		ft_pct			REAL DEFAULT 0,
		orb				REAL DEFAULT 0,
		drb				REAL DEFAULT 0,
		trb				REAL DEFAULT 0,
		ast				REAL DEFAULT 0,
		stl				REAL DEFAULT 0,
		blk				REAL DEFAULT 0,
		tov				REAL DEFAULT 0,
		pf				REAL DEFAULT 0,
		pts				REAL DEFAULT 0,
		
		inactive 		REAL		DEFAULT 0,
		threepar		REAL 		DEFAULT 0,
		sp				REAL		DEFAULT 0,
		ts_pct			REAL		DEFAULT 0,
		ftr				REAL		DEFAULT 0,
		orb_pct			REAL		DEFAULT 0,
		drb_pct			REAL		DEFAULT 0,
		trb_pct			REAL		DEFAULT 0,
		ast_pct			REAL		DEFAULT 0,
		stl_pct			REAL		DEFAULT 0,
		blk_pct			REAL		DEFAULT 0,	
		tov_pct			REAL		DEFAULT 0,
		usg_pct			REAL		DEFAULT 0,
		ortg			REAL		DEFAULT 0,
		drtg 			REAL		DEFAULT 0,
		bpm				REAL		DEFAULT 0,
		starter 		SMALLINT	DEFAULT 0,
		pm				REAL		DEFAULT 0
		);'''

create_season_table =\
	'''CREATE TABLE IF NOT EXISTS season(
		season		SMALLINT	NOT NULL,
		start_date	DATE		NOT NULL,
		end_date 	DATE 		NOT NULL,
		PRIMARY KEY(season)
		);'''
		
create_arena_table = \
	'''CREATE TABLE IF NOT EXISTS arena(
		team_id					SMALLINT	NOT NULL,
		home_arena_elevation	REAL		DEFAULT 0,
		active					SMALLINT	DEFAULT 0,
		inactive				SMALLINT 	DEFAULT 3000,
		PRIMARY KEY(team_id)
		);'''

create_team_table = \
	'''CREATE TABLE IF NOT EXISTS team(
		team_id					SMALLINT	NOT NULL,
		team_abbr				TEXT		NOT NULL,
		team_name				TEXT		NOT NULL,
		active					SMALLINT	DEFAULT 0,
		inactive				SMALLINT 	DEFAULT 3000,
		PRIMARY KEY(team_name, team_id, active, inactive)
		);'''
	
create_player_table = \
	'''CREATE TABLE IF NOT EXISTS player(
		player_id				SERIAL		NOT NULL,
		player_name				TEXT		NOT NULL,
		bbref_endpoint			TEXT		NOT NULL,
		PRIMARY KEY(player_id)
		);'''

#TODO: Modify season as foreign key
create_player_team_table = \
	'''CREATE TABLE IF NOT EXISTS player_team(
		player_id		SERIAL		NOT NULL,
		team_abbr		TEXT		NOT NULL,
		season			SMALLINT	NOT NULL,
		age				REAL,
		lg				TEXT,
		pos				TEXT,
		g				REAL,
		gs				REAL,
		mp				REAL,
		fg				REAL,
		fga				REAL,
		fg_pct			REAL,
		threep			REAL,
		threepa			REAL,
		threep_pct		REAL,
		twop			REAL,
		twopa			REAL,
		twop_pct		REAL,
		efg_pct			REAL,
		ft				REAL,
		fta				REAL,
		ft_pct			REAL,
		orb				REAL,
		drb				REAL,
		trb				REAL,
		ast				REAL,
		stl				REAL,
		blk				REAL,
		tov				REAL,
		pf				REAL,
		pts				REAL,
		PRIMARY KEY(player_id, team_abbr, season),
		FOREIGN KEY (player_id) REFERENCES player
		);'''

create_match_table = \
	'''CREATE TABLE IF NOT EXISTS match(
		match_id		SERIAL		NOT NULL,
		date			DATE		NOT NULL,
		away_pts		REAL		NOT NULL,
		home_pts		REAL		NOT NULL,
		away_abbr 		TEXT		NOT NULL,
		home_abbr		TEXT		NOT NULL,
		playoff_game	REAL		NOT NULL,
		elevation		REAL		DEFAULT 0,
		PRIMARY KEY (match_id)
		);'''

create_injury_table = \
	'''CREATE TABLE IF NOT EXISTS injury(
		player_id		SERIAL		NOT NULL,
		match_id		SERIAL		NOT NULL,
		injury			TEXT				,
		PRIMARY KEY (player_id, match_id),
		FOREIGN KEY (player_id) REFERENCES player,
		FOREIGN KEY (match_id) REFERENCES match
		);'''

create_player_performance_table = \
	'''CREATE TABLE IF NOT EXISTS player_performance(
		player_id		SERIAL		NOT NULL,
		match_id		SERIAL		NOT NULL,
		team_abbr		TEXT		NOT NULL,

		sp				REAL		DEFAULT 0,
		inactive 		REAL		DEFAULT 0,
		ts_pct			REAL		DEFAULT 0,
		efg_pct			REAL		DEFAULT 0,
		threepar		REAL 		DEFAULT 0,
		ftr				REAL		DEFAULT 0,
		orb_pct			REAL		DEFAULT 0,
		drb_pct			REAL		DEFAULT 0,
		trb_pct			REAL		DEFAULT 0,
		ast_pct			REAL		DEFAULT 0,
		stl_pct			REAL		DEFAULT 0,
		blk_pct			REAL		DEFAULT 0,	
		tov_pct			REAL		DEFAULT 0,
		usg_pct			REAL		DEFAULT 0,
		ortg			REAL		DEFAULT 0,
		drtg 			REAL		DEFAULT 0,
		bpm				REAL		DEFAULT 0,
		starter 		SMALLINT	DEFAULT 0,
		date			DATE		NOT NULL,
		fg				REAL		DEFAULT 0,
		fga				REAL		DEFAULT 0,
		fg_pct			REAL		DEFAULT 0,
		threep			REAL		DEFAULT 0,
		threepa			REAL		DEFAULT 0,
		threep_pct		REAL		DEFAULT 0,
		ft				REAL		DEFAULT 0,			
		fta				REAL		DEFAULT 0,
		ft_pct			REAL		DEFAULT 0,	
		orb				REAL		DEFAULT 0,
		drb				REAL		DEFAULT 0,
		trb				REAL		DEFAULT 0,
		ast				REAL		DEFAULT 0,
		stl				REAL		DEFAULT 0,
		blk				REAL		DEFAULT 0,		
		tov				REAL		DEFAULT 0,
		pf				REAL		DEFAULT 0,
		pts				REAL		DEFAULT 0,
		pm				REAL		DEFAULT 0,
		PRIMARY KEY (player_id, match_id), 
		FOREIGN KEY (player_id) REFERENCES player,
		FOREIGN KEY (match_id) REFERENCES match
		);'''
