import psycopg2


def connect(user, password, dbname, host="127.0.0.1", autocommit=False):
    conn = None
    try:
        conn = psycopg2.connect(f"host={host} port=9000 dbname={dbname} user={user} password={password}")
        conn.set_session(autocommit=autocommit)
    except psycopg2.Error as e:
        print(f"Error: could not make a connection to {dbname}")
        print(e)
    return conn


def getCursor(connection):
    cursor = None
    try:
        cursor = connection.cursor()
    except psycopg2.Error as e:
        print("Error: could not get a cursor")
        print(e)
    return cursor


def execute(cursor, command, *args):
    try:
        cursor.execute(command, *args)
    except psycopg2.Error as e:
        print(f"Error: could not execute command={command}")
        print(e)


def createTable(cursor, tablename, schema):
    execute(cursor, f"CREATE TABLE IF NOT EXISTS {tablename} ({schema});")


def insert(cursor, tablename, schema, values):
    value_string = f"{' '.join(['%s']*len(values)).strip().replace(' ',', ')}"
    execute(cursor, f"INSERT INTO {tablename} ({schema}) VALUES ({value_string})", values)
    #value_string = f"{*values,}".replace('[','ARRAY (').replace(']',')')
    #execute(cursor, f"INSERT INTO {tablename} ({schema}) VALUES {value_string};")
    #execute(cursor, f"INSERT INTO {tablename} ({schema}) VALUES ({values[:-1]}, ARRAY {values[-1]});")


def select(cursor, tablename, column='*'):
    execute(cursor, f"SELECT {column} FROM {tablename};")
