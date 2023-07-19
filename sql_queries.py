import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN=config.get('IAM_ROLE','ARN')

# DROP TABLES

staging_events_table_drop = 'DROP TABLE IF EXISTS "staging_events";'
staging_songs_table_drop =  'DROP TABLE IF EXISTS "staging_songs";'
songplay_table_drop = 'DROP TABLE IF EXISTS "songplays";'
user_table_drop = 'DROP TABLE IF EXISTS "users";'
song_table_drop = 'DROP TABLE IF EXISTS "songs";'
artist_table_drop = 'DROP TABLE IF EXISTS "artists";'
time_table_drop = 'DROP TABLE IF EXISTS "time";'

# CREATE TABLES

staging_events_table_create= ('''
CREATE TABLE staging_events (
    artist           VARCHAR,
    auth             VARCHAR,
    firstName        VARCHAR,
    gender           VARCHAR,
    itemInSession    INT,
    lastName         VARCHAR,
    length           FLOAT,
    level            VARCHAR,
    location         VARCHAR,
    method           VARCHAR,
    page             VARCHAR,
    registration     TIMESTAMP,
    sessionId        INT,
    song             VARCHAR,
    status           INT,
    ts               TIMESTAMP,
    userAgent        VARCHAR,
    userId           INT
    
);
''')

staging_songs_table_create = ('''
CREATE TABLE staging_songs(
    artist_id           VARCHAR,
    artist_latitude     VARCHAR,
    artist_location     VARCHAR,
    artist_longitude    VARCHAR,
    artist_name         VARCHAR,
    duration            DECIMAL,
    num_songs           INT,
    song_id             VARCHAR,
    title               VARCHAR,
    year                INT
);
''')

songplay_table_create = ('''
CREATE TABLE songplays (
    songplay_id INT PRIMARY KEY IDENTITY(0,1),
    start_time  TIMESTAMP, 
    user_id     INT, 
    level       VARCHAR, 
    song_id     VARCHAR, 
    artist_id   VARCHAR,
    session_id  INT, 
    location    VARCHAR, 
    user_agent  VARCHAR,
    FOREIGN KEY (start_time) REFERENCES time (start_time),
    FOREIGN KEY (user_id)    REFERENCES users (user_id),
    FOREIGN KEY (song_id)    REFERENCES songs (song_id),
    FOREIGN KEY (artist_id)  REFERENCES artists (artist_id)
);
''')

user_table_create = ('''
CREATE TABLE  users (
    user_id     INT PRIMARY KEY,
    first_name  VARCHAR,
    last_name   VARCHAR,
    gender      VARCHAR,
    level       VARCHAR
);
''')

song_table_create = ('''
CREATE TABLE songs (
    song_id    VARCHAR PRIMARY KEY,
    title      VARCHAR,
    artist_id  VARCHAR,
    year       INT,
    duration   DECIMAL
);
''')

artist_table_create = ('''
CREATE TABLE artists (
    artist_id     VARCHAR PRIMARY KEY,
    name          VARCHAR,
    location      VARCHAR,
    longitude     VARCHAR,
    latitude      VARCHAR
);
''')

time_table_create = ('''
CREATE TABLE time (
    start_time    TIMESTAMP PRIMARY KEY, 
    hour          INT, 
    day           INT, 
    week          INT, 
    month         INT,        
    year          INT, 
    weekday       INT
);
''')

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from 's3://udacity-dend/log_data/2018/'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    FORMAT AS JSON 'auto'
    TIMEFORMAT AS 'epochmillisecs';
""").format(ARN)

staging_songs_copy = ("""
    copy staging_songs from 's3://udacity-dend/song_data/A/'
    credentials 'aws_iam_role={}'
    region 'us-west-2'
    FORMAT AS JSON 'auto'
    TIMEFORMAT AS 'epochmillisecs';
""").format(ARN)

# FINAL TABLES

songplay_table_insert = ('''
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT e.ts, e.userId, e.level, s.song_id, s.artist_id, e.sessionId, e.location, e.userAgent 
FROM staging_songs    AS      s 
JOIN staging_events   AS      e 
ON e.song = s.title  
WHERE e.page = 'NextSong'; 
''')


user_table_insert = ('''
INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userid, firstname, lastname, gender, level
FROM staging_events    
WHERE userid IS NOT NULL AND page = 'NextSong';
''')

song_table_insert = ('''
INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT song_id, title, artist_id, year, duration 
FROM staging_songs ; 
''')

artist_table_insert = ('''
INSERT INTO artists (artist_id, name, location, longitude, latitude)
SELECT artist_id, artist_name, artist_location, artist_longitude, artist_latitude
FROM staging_songs 
WHERE artist_id IS NOT NULL;  
''')

time_table_insert = ('''
INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT(ts), EXTRACT(hour FROM ts), EXTRACT(day FROM ts), EXTRACT(week FROM ts), EXTRACT(month FROM ts), EXTRACT(year FROM ts), EXTRACT(weekday FROM ts)
FROM staging_events;
''')

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
