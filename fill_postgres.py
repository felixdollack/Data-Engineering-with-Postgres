import os
import glob
import psycopg2
import pandas as pd
import dbhelpers
from sql_queries import *
from dotenv import load_dotenv


def get_files(filepath):
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))
    return all_files


def insert_artist_and_songs(cur, filename):
    df = pd.read_json(filename, lines=True)
    song_data = list(df.values[0, [7, 8, 0, 9, 5]])
    artist_data = list(df.values[0, [0, 4, 2, 1, 3]])
    for (song, artist) in zip(song_data, artist_data):
        song[1] = song[1].replace("'", " ")
        dbhelpers.execute(cur, song_table_insert, song)
        dbhelpers.execute(cur, artist_table_insert, artist)


def insert_log_data(cur, filename):
    df = pd.read_json(filename, lines=True)
    df = df[df.page == "NextSong"]
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]]

    for i, row in user_df.iterrows():
        dbhelpers.execute(cur, user_table_insert, row)

    t = pd.to_datetime(df.ts, unit='ms')
    time_data = zip(
        t.dt.values, t.dt.hour.values, t.dt.day.values,
        t.dt.week.values, t.dt.month.values, t.dt.year.values,
        t.dt.weekday.values
    )
    column_labels = (
        'start_time', 'hour', 'day', 'week', 'month',
        'year', 'weekday'
    )

    time_df = pd.DataFrame(time_data, columns=column_labels)

    for i, row in time_df.iterrows():
        dbhelpers.execute(cur, time_table_insert, list(row))


if __name__ == "__main__":
    load_dotenv()
    user = os.getenv('DB_USER')
    passwd = os.getenv('DB_PASSWORD')
    db = os.getenv('DB_NAME')
    connection = dbhelpers.connect(user, passwd, db, host="127.0.0.1", autocommit=True)
    cursor = dbhelpers.getCursor(connection)

    song_files = get_files('data/song_data')
    for filename in song_files:
        insert_artist_and_songs(cursor, filename)

    log_files = get_files('data/log_data')
    for filename in log_files:
        insert_log_data(cursor, filename)
