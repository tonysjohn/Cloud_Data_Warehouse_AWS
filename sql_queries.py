import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = config.get("S3","LOG_DATA")
ARN = config.get("IAM_ROLE","ARN")
SONG_DATA = config.get("S3","SONG_DATA")
LOG_JSONPATH = config.get("S3","LOG_JSONPATH")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= """CREATE TABLE IF NOT EXISTS staging_events
                                 (
                                 artist TEXT,
                                 auth VARCHAR(20),
                                 firstName VARCHAR(100),
                                 gender CHAR,
                                 itemInSession INTEGER,
                                 lastName VARCHAR(100),
                                 length DOUBLE PRECISION,
                                 level VARCHAR(4),
                                 location TEXT,
                                 method VARCHAR(5),
                                 page VARCHAR(100),
                                 registration VARCHAR(20),
                                 sessionId INTEGER,
                                 song TEXT,
                                 status INTEGER,
                                 ts DOUBLE PRECISION,
                                 userAgent TEXT,
                                 userId INTEGER
                                 )                                                                
"""

staging_songs_table_create = """CREATE TABLE IF NOT EXISTS staging_songs
                                 (num_songs INTEGER,
                                 artist_id VARCHAR(50),
                                 artist_latitude DOUBLE PRECISION,
                                 artist_longitude DOUBLE PRECISION,
                                 artist_location TEXT,
                                 artist_name TEXT,
                                 song_id VARCHAR(50),
                                 title TEXT,
                                 duration DOUBLE PRECISION,
                                 year INTEGER)
"""

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id INTEGER IDENTITY(0,1) NOT NULL PRIMARY KEY
                                                                   , start_time TIMESTAMP NOT NULL SORTKEY
                                                                   , user_id INTEGER NOT NULL DISTKEY
                                                                   , level VARCHAR(10)
                                                                   , song_id VARCHAR(50)
                                                                   , artist_id VARCHAR(50)
                                                                   , session_id INTEGER
                                                                   , location TEXT
                                                                   , user_agent TEXT);
""")

user_table_create = (""" CREATE TABLE IF NOT EXISTS users(user_id int NOT NULL PRIMARY KEY
                                           , first_name VARCHAR(100)
                                           , last_name VARCHAR(100)
                                           , gender CHAR
                                           , level VARCHAR(10) NOT NULL);
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs(song_id VARCHAR(50) NOT NULL PRIMARY KEY
                                           , title TEXT
                                           , artist_id VARCHAR(50)
                                           , year INTEGER
                                           , duration INTEGER);
""")

artist_table_create = (""" CREATE TABLE IF NOT EXISTS artists(artist_id VARCHAR(50) NOT NULL PRIMARY KEY
                                              , name TEXT
                                              , location TEXT
                                              , latitude DOUBLE PRECISION
                                              , longitude DOUBLE PRECISION);
""")

time_table_create = (""" CREATE TABLE IF NOT EXISTS time(start_time TIMESTAMP NOT NULL PRIMARY KEY
                                           , hour INTEGER
                                           , day INTEGER
                                           , week INTEGER
                                           , month INTEGER
                                           , year INTEGER
                                           , weekday INTEGER);
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM {}
                          CREDENTIALS 'aws_iam_role={}'
                          region 'us-west-2'
                          format as json {}
                          maxerror as 10
""").format(LOG_DATA, ARN,LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs FROM {}
                         CREDENTIALS 'aws_iam_role={}'
                         region 'us-west-2'
                         format as json 'auto'
                         maxerror as 10
                         --STATUPDATE OFF
""").format(SONG_DATA, ARN)

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays(start_time
                                                   , user_id
                                                   , level
                                                   , song_id
                                                   , artist_id
                                                   , session_id
                                                   , location
                                                   , user_agent)
                             SELECT to_timestamp('1970-01-01'::date + ts/1000 * interval '1 second','YYYY-MM-DD HH24:MI:SS.MS')
                                   , e.userId
                                   , e.level
                                   , s.song_id
                                   , s.artist_id
                                   , e.sessionId
                                   , e.location
                                   , e.userAgent                                    
                                  FROM staging_events e LEFT JOIN staging_songs s
                                  ON lower(TRIM(BOTH FROM e.song)) = lower(TRIM(BOTH FROM s.title))
                                  AND lower(TRIM(BOTH FROM e.artist)) = lower(TRIM(BOTH FROM s.artist_name))
                                  AND CAST(e.length as INTEGER) = CAST(s.duration as INTEGER)
                                  WHERE page = 'NextSong'
""")

user_table_insert = ("""DELETE FROM users
                        WHERE user_id in (SELECT DISTINCT userId FROM staging_events);
                        
                        INSERT INTO users(user_id
                                           , first_name
                                           , last_name
                                           , gender
                                           , level)
                                    SELECT userId
                                           , firstName
                                           , lastName
                                           , gender
                                           , level
                                    FROM(
                                    SELECT userId
                                           , firstName
                                           , lastName
                                           , gender
                                           , level
                                           , ROW_NUMBER() OVER(PARTITION BY userid ORDER BY ts DESC) as row
                                    FROM staging_events e
                                    WHERE page = 'NextSong') c
                                    WHERE c.row = 1;
""")

song_table_insert = ("""DELETE FROM songs
                        WHERE song_id in (SELECT DISTINCT song_id FROM staging_songs);
                        
                        INSERT INTO songs(song_id
                                        , title
                                        , artist_id
                                        , year
                                        , duration)
                                    SELECT DISTINCT song_id
                                        , title
                                        , artist_id
                                        , year
                                        , duration
                                    FROM staging_songs;
""")

artist_table_insert = ("""DELETE FROM artists
                        WHERE artist_id in (SELECT DISTINCT artist_id FROM staging_songs);
                        INSERT INTO artists(artist_id
                                              , name
                                              , location
                                              , latitude
                                              , longitude)
                                      SELECT DISTINCT artist_id
                                              , artist_name
                                              , artist_location
                                              , artist_latitude
                                              , artist_longitude
                                      FROM staging_songs
""")

time_table_insert = ("""INSERT INTO time(start_time
                                           , hour
                                           , day
                                           , week
                                           , month
                                           , year
                                           , weekday)
                                      SELECT ts_clean
                                           , EXTRACT(HOUR FROM ts_clean)
                                           , EXTRACT(DAY FROM ts_clean)
                                           , EXTRACT(WEEK FROM ts_clean)
                                           , EXTRACT(MONTH FROM ts_clean)
                                           , EXTRACT(YEAR FROM ts_clean)
                                           , EXTRACT(DOW FROM ts_clean)
                                     FROM 
                                     (SELECT DISTINCT 
                                     to_timestamp('1970-01-01'::date + ts/1000 * interval '1 second','YYYY-MM-DD HH24:MI:SS.MS') as ts_clean
                                     FROM staging_events
                                     WHERE page = 'NextSong')
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
