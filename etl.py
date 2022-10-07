import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
import dbhelpers
from dotenv import load_dotenv


def process_song_file(cur, filepath):
    """
    Opens a song file, extracts the data,
    and fills it into the songs and artists tables.

        Parameters:
            cur      (object): A database cursor
            filepath (string): Path to a song metadata file
    """
    df = pd.read_json(filepath, lines=True)

    song_data = df[[
        'song_id', 'title', 'artist_id', 'year', 'duration'
    ]]
    for kk, row in song_data.iterrows():
        cur.execute(song_table_insert, list(row))

    artist_data = df[[
        'artist_id', 'artist_name', 'artist_location',
        'artist_latitude', 'artist_longitude'
    ]]
    for kk, row in artist_data.iterrows():
        cur.execute(artist_table_insert, list(row))


def process_log_file(cur, filepath):
    """
    Opens a log file and extracts the data related to song changes.
    Timestamps are converted from milliseconds to datetime and filled in the times table.
    User data is filled into the users table.
    To populate the songplays table, data from songs and artists is called in a joint query.

        Parameters:
            cur      (object): A database cursor
            filepath (string): Path to a user log file
    """

    df = pd.read_json(filepath, lines=True)
    df = df[df.page == 'NextSong']

    t = pd.to_datetime(df.ts, unit='ms')

    time_data = [
        t, t.dt.hour, t.dt.day, t.dt.week,
        t.dt.month, t.dt.year, t.dt.weekday
    ]
    column_labels = [
        'start_time', 'hour', 'day', 'week',
        'month', 'year', 'weekday'
    ]
    time_df = pd.DataFrame(
        list(zip(*time_data)), columns=column_labels
    )

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    user_df = df[[
        'userId','firstName','lastName','gender','level'
    ]]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (
            pd.to_datetime(row.ts, unit='ms'),
            row.userId, row.level, songid, artistid,
            row.sessionId, row.location, row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    List all files in the path recursively,
    call the provided function,
    and finally commit all changes to the database.

        Parameters:
            cur      (object): A database cursor
            conn     (object): A database connection
            filepath (string): Path to a folder with data
            func     (object): A function to process the data in the folder
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    The main function.
    Will connect to the database and call the process function
    to ingest data about songs and user behavior.
    """

    load_dotenv()
    user = os.getenv('DB_USER')
    passwd = os.getenv('DB_PASSWORD')
    db = os.getenv('DB_NAME')
    conn = dbhelpers.connect(user, passwd, db, host="127.0.0.1", autocommit=True)
    #conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
