import sqlite3

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from extractor import SQLiteExtractor
from transformer import SQLiteToPGTransformer
from saver import PostgresSaver
from config import PG_DSL, SQLITE_DB_PATH


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    sqlite_extractor = SQLiteExtractor(connection)
    transformer = SQLiteToPGTransformer()
    postgres_saver = PostgresSaver(pg_conn)
    
    sqlite_data = sqlite_extractor.extract_movies()
    
    transformer.sqlite_data = sqlite_data
    pg_data = transformer.transform()
    
    postgres_saver.save_all_data(pg_data)


if __name__ == "__main__":
    print(PG_DSL)
    print(SQLITE_DB_PATH)
    with sqlite3.connect(SQLITE_DB_PATH) as sqlite_conn, psycopg2.connect(
        **PG_DSL, cursor_factory=DictCursor
    ) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
