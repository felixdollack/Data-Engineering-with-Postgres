from sql_queries import create_table_queries, drop_table_queries
from dbhelpers import connect, getCursor, createTable, execute
from dotenv import load_dotenv
import os


def main(user, password, database):
    connection = connect(user, password, database, host="127.0.0.1", autocommit=True)
    cursor = getCursor(connection)
    for table in zip(drop_table_queries, create_table_queries):
        execute(cursor, table[0])
        execute(cursor, table[1])


if __name__ == '__main__':
    load_dotenv()
    user = os.getenv('DB_USER')
    passwd = os.getenv('DB_PASSWORD')
    db = os.getenv('DB_NAME')
    main(user, passwd, db)
