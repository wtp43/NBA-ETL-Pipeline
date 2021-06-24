create_season_table =\
	'''CREATE TABLE IF NOT EXISTS season(
		year	SMALLINT	NOT NULL,
		PRIMARY KEY (year)
		);'''
		
create_team_table = \
	'''CREATE TABLE IF NOT EXISTS team(
		team_id					SERIAL		NOT NULL,
		team_name				TEXT		NOT NULL,
		symbol					CHAR(3)		NOT NULL,
		home_arena_elevation	REAL		DEFAULT 0,
		created					SMALLINT	DEFAULT 0
		inactive				SMALLINT 	DEFAULT 3000,
		PRIMARY KEY(team_id)
		);'''

create_player_table = \
	'''CREATE TABLE IF NOT EXISTS team(
		player_id					SERIAL		NOT NULL,
		player_name					TEXT		NOT NULL,
		PRIMARY KEY(player_id)
		);'''

create_player_team_table = \
	'''CREATE TABLE IF NOT EXISTS team(
		player_id					SERIAL		NOT NULL,
		team_id						SERIAL		NOT NULL,
		year						SMALLINT	NOT NULL,
		PRIMARY KEY(player_id, team_id, year),
		FOREIGN KEY (player_id) REFERENCES player,
		FOREIGN KEY (team_id) REFERENCES team
		);'''

create_match_table = \
	'''CREATE TABLE IF NOT EXISTS match(
		match_id		SERIAL		NOT NULL,
		date			DATE		NOT NULL,
		away_pts		REAL		NOT NULL,
		home_pts		REAL		NOT NULL,
		away_id 		SERIAL		NOT NULL,
		home_id			SERIAL		NOT NULL,
		elevation		REAL		DEFAULT 0,
		PRIMARY KEY (match_id),
		FOREIGN KEY (away_id) REFERENCES team,
		FOREIGN KEY (home_id) REFERENCES team
		);'''

create_injury_table = \
	'''CREATE TABLE IF NOT EXISTS match(
		player_id		SERIAL		NOT NULL,
		match_id		DATE		NOT NULL,
		injury			TEXT				,
		PRIMARY KEY (player_id, match_id),
		FOREIGN KEY (player_id) REFERENCES player,
		FOREIGN KEY (match_id) REFERENCES match,
		);'''


create_player_performance_import_table = \
	'''CREATE UNLOGGED TABLE IF NOT EXISTS player_performance_imports(
		player_name		TEXT		NOT NULL,
		ts_p			REAL		DEFAULT 0,
		efg_p			REAL		DEFAULT 0,
		three_par		REAL 		DEFAULT 0,
		ftr				REAL		DEFAULT 0,
		orb_p			REAL		DEFAULT 0,
		drb_p			REAL		DEFAULT 0,
		trb_p			REAL		DEFAULT 0,
		ast_p			REAL		DEFAULT 0,
		stl_p			REAL		DEFAULT 0,
		blk_p			REAL		DEFAULT 0,	
		tov_p			REAL		DEFAULT 0,
		usg_p			REAL		DEFAULT 0,
		ortg			REAL		DEFAULT 0,
		drtg 			REAL		DEFAULT 0,
		bpm				REAL		DEFAULT 0,
		match_id		SERIAL		NOT NULL,
		starter 		SMALLINT	DEFAULT 0,
		date			DATE		NOT NULL,
		fg				INTEGER		DEFAULT 0,
		fga				INTEGER		DEFAULT 0,
		fg_p			REAL		DEFAULT 0,
		three_p			INTEGER		DEFAULT 0,
		three_pa		INTEGER		DEFAULT 0,
		three_p_p		REAL		DEFAULT 0,
		ft				INTEGER		DEFAULT 0,			
		fta				INTEGER		DEFAULT 0,
		ft_p			REAL		DEFAULT 0,	
		orb				INTEGER		DEFAULT 0,
		drb				INTEGER		DEFAULT 0,
		trb				INTEGER		DEFAULT 0,
		ast				INTEGER		DEFAULT 0,
		stl				INTEGER		DEFAULT 0,
		blk				INTEGER		DEFAULT 0,		
		tov				INTEGER		DEFAULT 0,
		pf				INTEGER		DEFAULT 0,
		pts				INTEGER		DEFAULT 0,
		pm				INTEGER		DEFAULT 0
		);'''

create_match_import_table = \
	'''CREATE UNLOGGED TABLE IF NOT EXISTS match_imports(
		date			DATE		NOT NULL,
		away_pts		REAL		NOT NULL,
		home_pts		REAL		NOT NULL,
		away 			CHAR(3)		NOT NULL,
		home 			CHAR(3)		NOT NULL,
		elevation		REAL		DEFAULT 0
		);'''


create_player_performance_table = \
	'''CREATE TABLE IF NOT EXISTS player_performance(
		player_name		TEXT		NOT NULL,
		ts_p			REAL		DEFAULT 0,
		efg_p			REAL		DEFAULT 0,
		three_par		REAL 		DEFAULT 0,
		ftr				REAL		DEFAULT 0,
		orb_p			REAL		DEFAULT 0,
		drb_p			REAL		DEFAULT 0,
		trb_p			REAL		DEFAULT 0,
		ast_p			REAL		DEFAULT 0,
		stl_p			REAL		DEFAULT 0,
		blk_p			REAL		DEFAULT 0,	
		tov_p			REAL		DEFAULT 0,
		usg_p			REAL		DEFAULT 0,
		ortg			REAL		DEFAULT 0,
		drtg 			REAL		DEFAULT 0,
		bpm				REAL		DEFAULT 0,
		match_id		SERIAL		NOT NULL,
		starter 		SMALLINT	DEFAULT 0,
		date			DATE		NOT NULL,
		fg				INTEGER		DEFAULT 0,
		fga				INTEGER		DEFAULT 0,
		fg_p			REAL		DEFAULT 0,
		three_p			INTEGER		DEFAULT 0,
		three_pa		INTEGER		DEFAULT 0,
		three_p_p		REAL		DEFAULT 0,
		ft				INTEGER		DEFAULT 0,			
		fta				INTEGER		DEFAULT 0,
		ft_p			REAL		DEFAULT 0,	
		orb				INTEGER		DEFAULT 0,
		drb				INTEGER		DEFAULT 0,
		trb				INTEGER		DEFAULT 0,
		ast				INTEGER		DEFAULT 0,
		stl				INTEGER		DEFAULT 0,
		blk				INTEGER		DEFAULT 0,		
		tov				INTEGER		DEFAULT 0,
		pf				INTEGER		DEFAULT 0,
		pts				INTEGER		DEFAULT 0,
		pm				INTEGER		DEFAULT 0,
		FOREIGN KEY (match_id) REFERENCES match
		);'''
