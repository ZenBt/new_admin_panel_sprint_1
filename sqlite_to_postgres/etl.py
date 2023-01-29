import sqlite3

from extractor import SQLiteExtractor
from transformer import SQLiteToPGTransformer
from saver import PostgresSaver


class SQLiteToPGETL:
    def __init__(
        self,
        extractor: SQLiteExtractor,
        transformer: SQLiteToPGTransformer,
        saver: PostgresSaver,
    ) -> None:
        self._extractor = extractor
        self._transformer = transformer
        self._saver = saver

    def run(self, chunk_size: int = 1000):
        """Run ETL
        1. Extract data from SQLite DB
            (film_work, genre, person, genre_filmwork, person_filmworks)
        2. Transform data to Postgres format
        3. Save data to Postgres DB
            (filmwork, genre, person, genre_filmwork, person_filmworks)
        4. Repeat steps 1-3 until all data is extracted

        All data extracted and loaded in N sized rows chunks
        (N = chunk_size, default=1000)
        """
        self._extractor.set_chunk_size(chunk_size)
        sqlite_data = self._extractor.extract_movies()

        while sqlite_data:
            self._transformer.sqlite_data = sqlite_data
            pg_data = self._transformer.transform()
            self._saver.save_all_data(pg_data)
            sqlite_data = self._extractor.extract_movies()
