class SqlQueries:
    songplay_table_insert = ("""
        SELECT
          MD5(events.sessionid || events.start_time) AS songplay_id,
          events.start_time,
          events.userid,
          events.level,
          songs.song_id,
          songs.artist_id,
          events.sessionid,
          events.location,
          events.useragent
        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
              FROM staging_events
              WHERE page='NextSong') AS events
        LEFT JOIN staging_songs songs
               ON events.song = songs.title
              AND events.artist = songs.artist_name
              AND events.length = songs.duration
    """)

    user_table_insert = ("""
        SELECT DISTINCT userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_insert = ("""
        SELECT DISTINCT song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_insert = ("""
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_insert = ("""
        SELECT start_time, EXTRACT(HOUR FROM start_time), EXTRACT(DAY FROM start_time), EXTRACT(WEEK FROM start_time),
               EXTRACT(MONTH FROM start_time), EXTRACT(YEAR FROM start_time), EXTRACT(DAYOFWEEK FROM start_time)
        FROM songplays
    """)
