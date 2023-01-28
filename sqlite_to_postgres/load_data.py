import sqlite3

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from .extractor import SQLiteExtractor
from .saver import PostgresSaver
from .config import PG_DSL, SQLITE_DB_PATH


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    postgres_saver = PostgresSaver(pg_conn)
    sqlite_extractor = SQLiteExtractor(connection)

    data = sqlite_extractor.extract_movies()
    postgres_saver.save_all_data(data)


if __name__ == "__main__":
    with sqlite3.connect(SQLITE_DB_PATH) as sqlite_conn, psycopg2.connect(
        **PG_DSL, cursor_factory=DictCursor
    ) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
