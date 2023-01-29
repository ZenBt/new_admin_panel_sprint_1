from typing import Protocol, Type
import sqlite3

from psycopg2.extensions import connection as _connection

from extractor import (
    SQLiteExtractor,
    RelationalSQLiteExtractor,
    BaseSQLiteExtractor,
)
from transformer import (
    SQLiteToPGTransformer,
    RelationalSQLiteToPGTransformer,
    BaseTransformer,
)
from saver import PostgresSaver, RelationalPostgresSaver, BasePostgresSaver


class ETL(Protocol):
    def run(self, chunk_size: int = 1000) -> None:
        ...


class ConcreteETL(ETL):
    def __init__(
        self,
        extractor: BaseSQLiteExtractor,
        transformer: BaseTransformer,
        saver: BasePostgresSaver,
    ) -> None:
        self._extractor = extractor
        self._transformer = transformer
        self._saver = saver

    def run(self, chunk_size: int = 1000) -> None:
        """Run ETL
        1. Extract data from SQLite DB
        2. Transform data to Postgres format
        3. Save data to Postgres DB
        4. Repeat steps 1-3 until all data is extracted

        All data extracted and loaded in N sized rows chunks
        (N = chunk_size, default=1000)
        """
        self._extractor.set_chunk_size(chunk_size)
        sqlite_data = self._extractor.extract()

        while sqlite_data:
            self._transformer.sqlite_data = sqlite_data
            pg_data = self._transformer.transform()
            self._saver.save_all_data(pg_data)
            sqlite_data = self._extractor.extract()


class SQLiteToPGETL(ConcreteETL):
    def __init__(
        self,
        extractor: SQLiteExtractor,
        transformer: SQLiteToPGTransformer,
        saver: PostgresSaver,
    ) -> None:
        super().__init__(extractor, transformer, saver)


class RelationalSQLiteToPGETL(ConcreteETL):
    def __init__(
        self,
        extractor: RelationalSQLiteExtractor,
        transformer: RelationalSQLiteToPGTransformer,
        saver: RelationalPostgresSaver,
    ) -> None:
        super().__init__(extractor, transformer, saver)


class MultiStageETL(ETL):
    def __init__(self, connection: sqlite3.Connection, pg_conn: _connection) -> None:
        self._connection = connection
        self._pg_conn = pg_conn

    def run(self, chunk_size: int = 1000) -> None:
        """Run multiple ETLs
        1. Extract, transform and save data from SQLite DB to Postgres DB
        for tables with no relations (filmwork, genre, person)
        2. Extract, transform and save data from SQLite DB to Postgres DB
        for tables with relations (genre_filmwork, person_film_works)

        All data extracted and loaded in N sized rows chunks
        (N = chunk_size, default=1000)
        """
        self._set_chunk_size(chunk_size)
        self._run_etl_for_primary_tables()
        self._run_etl_for_related_tables()

    def _run_etl_for_primary_tables(self) -> None:
        self._run_etl(
            extractor=SQLiteExtractor,
            transformer=SQLiteToPGTransformer,
            saver=PostgresSaver,
            concrete_etl=SQLiteToPGETL,
        )

    def _run_etl_for_related_tables(self) -> None:
        self._run_etl(
            extractor=RelationalSQLiteExtractor,
            transformer=RelationalSQLiteToPGTransformer,
            saver=RelationalPostgresSaver,
            concrete_etl=RelationalSQLiteToPGETL,
        )

    def _run_etl(
        self,
        extractor: Type[BaseSQLiteExtractor],
        transformer: Type[BaseTransformer],
        saver: Type[BasePostgresSaver],
        concrete_etl: Type[ConcreteETL],
    ) -> None:
        
        extractor = extractor(self._connection)
        transformer = transformer()
        saver = saver(self._pg_conn.cursor())

        etl = concrete_etl(extractor, transformer, saver)
        etl.run(self.chunk_size)
    
    def _set_chunk_size(self, chunk_size: int) -> None:
        self.chunk_size = chunk_size
