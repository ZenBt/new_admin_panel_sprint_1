import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from sqlite3 import Cursor, connect, Error
from dataclasses import fields as dataclass_fields
from typing import Any, Generator
from config import CHUNK_SIZE


class BaseSQLiteExtractor(ABC):
    def __init__(self, cursor: Cursor, table: str) -> None:
        self._cur = cursor
        self._table = table
        self._chunk_size = CHUNK_SIZE

    @abstractmethod
    def extract(self) -> list[dict]:
        pass

    def set_chunk_size(self, chunk_size: int) -> None:
        self._chunk_size = chunk_size

    def _get_dataclass_fields(self, dataclass: type) -> tuple[str, ...]:
        return tuple(field.name for field in dataclass_fields(dataclass))


class SQLiteMovieExtractor(BaseSQLiteExtractor):

    BASE_SQL = """
        SELECT
            *
        FROM
            {table_name}
        ORDER BY
            id
        """

    def extract(self) -> Generator[dict, None, None]:
        sql = self.BASE_SQL.format(table_name=self._table)
        try:
            self._cur.execute(sql)
        except Error as e:
            logging.error(f"Error occurred while extracting data: {e}")
            raise e

        while True:
            rows = self._cur.fetchmany(size=self._chunk_size)

            if not rows:
                return

            yield from rows


@contextmanager
def connect_to_sqlite3(file_name: str):
    conn = connect(file_name)
    conn.row_factory = _dict_cursor_factory
    try:
        logging.debug("Creating connection")
        yield conn.cursor()
    finally:
        logging.debug("Closing connection")
        conn.commit()
        conn.close()


def _dict_cursor_factory(cursor: Cursor, row: list) -> dict:
    data = {}
    for index, column in enumerate(cursor.description):
        data[column[0]] = row[index]
    return data
