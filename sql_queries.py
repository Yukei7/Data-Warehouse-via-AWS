import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events_table
(
artist_name VARCHAR,
auth VARCHAR,
firstName VARCHAR,
gender CHAR(1),
itemInSession INT,
lastName VARCHAR,
length DECIMAL(8,4),
level VARCHAR,
location VARCHAR,
method VARCHAR,
page VARCHAR,
registration VARCHAR,
sessionId INT,
song VARCHAR,
status INT,
ts TIMESTAMP,
userAgent VARCHAR,
userId INT
)
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs_table
(
num_songs INT,
artist_id VARCHAR(18),
artist_latitude DECIMAL(8,4),
artist_longitude DECIMAL(8,4),
artist_name VARCHAR,
song_id VARCHAR(18),
title VARCHAR,
duration DECIMAL(8,4),
year INT
);
""")

# songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
# Redshift don't have SERIAL type
# set songplay_id as SERIAL type to automatically generate the id number
songplay_table_create = ("""
CREATE TABLE songplays 
(songplay_id INT IDENTITY(0,1) NOT NULL, 
start_time TIMESTAMP NOT NULL sortkey, 
user_id INT NOT NULL, 
level VARCHAR NOT NULL, 
song_id VARCHAR(18), 
artist_id VARCHAR(18), 
session_id INT NOT NULL, 
location VARCHAR NOT NULL, 
user_agent VARCHAR NOT NULL);
""")

# user_id, first_name, last_name, gender, level
user_table_create = ("""
CREATE TABLE users
(user_id INT NOT NULL sortkey, 
first_name VARCHAR NOT NULL, 
last_name VARCHAR NOT NULL, 
gender CHAR(1) NOT NULL, 
level VARCHAR NOT NULL);
""")

# song_id, title, artist_id, year, duration
song_table_create = ("""
CREATE TABLE songs
(song_id VARCHAR(18) NOT NULL sortkey, 
title VARCHAR NOT NULL, 
artist_id VARCHAR(18) NOT NULL, 
year INT NOT NULL, 
duration DECIMAL(8,4) NOT NULL);
""")

# artist_id, name, location, latitude, longitude
artist_table_create = ("""
CREATE TABLE artists
(artist_id VARCHAR(18) NOT NULL sortkey,
name VARCHAR NOT NULL,
location VARCHAR NOT NULL,
latitude DECIMAL(8,4),
longitude DECIMAL(8,4));
""")

# start_time, hour, day, week, month, year, weekday
time_table_create = ("""
CREATE TABLE time
(start_time TIMESTAMP NOT NULL sortkey,
hour INT NOT NULL,
day INT NOT NULL,
week INT NOT NULL,
month INT NOT NULL,
year INT NOT NULL,
weekday INT NOT NULL);
""")


# STAGING TABLES

staging_events_copy = ("""
COPY staging_events_table FROM '{}'
IAM_ROLE '{}'
FORMAT AS json '{}'
TIMEFORMAT AS 'epochmillisecs'
REGION 'us-west-2';
""").format(config['S3']['LOG_DATA'], 
            config['IAM_ROLE']['ARN'],
            config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
COPY staging_songs_table FROM '{}'
IAM_ROLE '{}'
FORMAT AS json 'auto'
TIMEFORMAT AS 'epochmillisecs'
REGION 'us-west-2';
""").format(config['S3']['SONG_DATA'],
            config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays
(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT
        se.ts,
        se.userID,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionId,
        se.location,
        se.userAgent
    FROM staging_events_table se
    JOIN staging_songs_table ss
    ON (se.artist_name = ss.artist_name)
    AND (se.song = ss.title)
    WHERE se.page = 'NextSong';
""")

user_table_insert = ("""
INSERT INTO users
(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT
        userID, firstName, lastName, gender, level
    FROM staging_events
    WHERE page='NextSong';
""")

song_table_insert = ("""
INSERT INTO songs
(song_id, title, artist_id, year, duration)
    SELECT DISTINCT
        song_id, title, artist_id, year, duration
    FROM staging_songs
""")

artist_table_insert = ("""
INSERT INTO artists
(artist_id, name, location, latitude, longitude)
    SELECT DISTINCT
        ss.artist_id,
        se.artist_name,
        se.location,
        ss.artist_latitude,
        ss.artist_longitude
    FROM staging_events_table se
    JOIN staging_songs_table ss
    ON (se.artist_name = ss.artist_name)
    AND (se.song = ss.title)
    WHERE se.page = 'NextSong';
""")

time_table_insert = ("""
INSERT INTO time
(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT
        ts,
        EXTRACT(HOUR FROM ts) AS hour,
        EXTRACT(DAY FROM ts) AS day,
        EXTRACT(WEEK FROM ts) AS week,
        EXTRACT(MONTH FROM ts) AS month,
        EXTRACT(YEAR FROM ts) AS year,
        EXTRACT(WEEKDAY FROM ts) AS weekday
    FROM staging_events
    WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
