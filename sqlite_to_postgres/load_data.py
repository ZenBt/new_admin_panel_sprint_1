import sqlite3

from psycopg2.extensions import cursor as _cursor
from psycopg2.extras import DictCursor

from config import PG_DSL, SQLITE_DB_PATH, CHUNK_SIZE
from etl import MultiStageETL
from extractor import connect_to_sqlite3
from saver import connect_to_postgres


def load_from_sqlite(sqlite_cur: sqlite3.Cursor, pg_cur: _cursor):
    """Основной метод загрузки данных из SQLite в Postgres"""
    etl = MultiStageETL(sqlite_cur, pg_cur)
    etl.set_chunk_size(CHUNK_SIZE)
    etl.run()


if __name__ == "__main__":
    with connect_to_sqlite3(SQLITE_DB_PATH) as sqlite_cur, connect_to_postgres(
        PG_DSL, DictCursor
    ) as pg_cur:
        load_from_sqlite(sqlite_cur, pg_cur)
