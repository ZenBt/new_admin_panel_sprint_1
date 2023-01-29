import sqlite3

import psycopg2
from psycopg2.extensions import connection as _connection
from psycopg2.extras import DictCursor

from config import PG_DSL, SQLITE_DB_PATH, CHUNK_SIZE
from etl import MultiStageETL


def load_from_sqlite(connection: sqlite3.Connection, pg_conn: _connection):
    """Основной метод загрузки данных из SQLite в Postgres"""
    etl = MultiStageETL(connection, pg_conn)
    etl.run(chunk_size=CHUNK_SIZE)


if __name__ == "__main__":
    with sqlite3.connect(SQLITE_DB_PATH) as sqlite_conn, psycopg2.connect(
        **PG_DSL, cursor_factory=DictCursor
    ) as pg_conn:
        load_from_sqlite(sqlite_conn, pg_conn)
