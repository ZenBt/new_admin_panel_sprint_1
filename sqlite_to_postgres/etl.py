from typing import Protocol

import sqlite3

from psycopg2.extensions import cursor as _cursor

from extractor import SQLiteMovieExtractor
from transformer import SQLiteToPGTransformer
from saver import PostgresSaver


class ETL(Protocol):

    TABLES: tuple[str, ...]

    def __init__(self, sqlite_cur: sqlite3.Cursor, pg_cur: _cursor) -> None:
        ...

    def run(self) -> None:
        ...
    
    def set_chunk_size(self, chunk_size: int) -> None:
        ...


class ConcreteETL(ETL):

    TABLES = tuple()

    def __init__(self, sqlite_cur: sqlite3.Cursor, pg_cur: _cursor) -> None:
        self._sqlite_cur = sqlite_cur
        self._pg_cur = pg_cur
        self._chunk_size = 1000

    def run(self) -> None:
        """Run ETL
        1. Extract data from SQLite DB
        2. Transform data to Postgres format
        3. Save data to Postgres DB
        4. Repeat steps 1-3 until all data is extracted

        All data extracted and loaded in N sized rows chunks
        (N = chunk_size, default=1000)
        """
        for table in self.TABLES:
            extractor = SQLiteMovieExtractor(self._sqlite_cur, table)
            extractor.set_chunk_size(self._chunk_size)

            for rows in extractor.extract():
                transformer = SQLiteToPGTransformer(rows, table)
                saver = PostgresSaver(self._pg_cur)
                saver.save(table, transformer.transform())
    
    def set_chunk_size(self, chunk_size: int) -> None:
        self._chunk_size = chunk_size
  


class SQLiteToPGETL(ConcreteETL):
    TABLES = ("film_work", "genre", "person")


class RelationalSQLiteToPGETL(ConcreteETL):
    TABLES = ("genre_film_work", "person_film_work")


class MultiStageETL(ETL):

    def __init__(self, sqlite_cur: sqlite3.Cursor, pg_cur: _cursor) -> None:
        self._sqlite_cur = sqlite_cur
        self._pg_cur = pg_cur
        self._chunk_size = 1000

    def run(self) -> None:
        """Run multiple ETLs
        1. Extract, transform and save data from SQLite DB to Postgres DB
        for tables with no relations (film_work, genre, person)
        2. Extract, transform and save data from SQLite DB to Postgres DB
        for tables with relations (genre_film_work, person_film_work)

        All data extracted and loaded in N sized rows chunks
        (N = chunk_size, default=1000)
        """
        self._run_etl_for_primary_tables(sqlite_cur=self._sqlite_cur, pg_cur=self._pg_cur)
        self._run_etl_for_related_tables(sqlite_cur=self._sqlite_cur, pg_cur=self._pg_cur)
    
    def set_chunk_size(self, chunk_size: int) -> None:
        self._chunk_size = chunk_size

    def _run_etl_for_primary_tables(
        self, sqlite_cur: sqlite3.Cursor, pg_cur: _cursor
    ) -> None:
        etl = SQLiteToPGETL(sqlite_cur, pg_cur)
        etl.set_chunk_size(self._chunk_size)
        etl.run()


    def _run_etl_for_related_tables(
        self, sqlite_cur: sqlite3.Cursor, pg_cur: _cursor
    ) -> None:
        etl = RelationalSQLiteToPGETL(sqlite_cur, pg_cur)
        etl.set_chunk_size(self._chunk_size)
        etl.run()
