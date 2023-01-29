from sqlite3 import Connection as SQLiteConnection

from dto import (
    SQLiteMovies,
    SQLitePerson,
    SQLiteFilmWork,
    SQLiteGenre,
    SQLiteGenreFilmwork,
    SQLitePersonFilmWork,
)


class SQLiteExtractor:
    def __init__(self, connection: SQLiteConnection) -> None:
        self._conn = connection

    def extract_movies(self) -> SQLiteMovies:
        """Extract movies from SQLite"""
        return

    def _extract_persons(self) -> list[SQLitePerson]:
        """Extract persons from SQLite"""
        return
    
    def _extract_filmworks(self) -> list[SQLiteFilmWork]:
        """Extract filmworks from SQLite"""
        return

    def _extract_genres(self) -> list[SQLiteGenre]:
        """Extract genres from SQLite"""
        return

    def _extract_genre_filmworks(self) -> list[SQLiteGenreFilmwork]:
        """Extract genre_filmworks from SQLite"""
        return
    
    def _extract_person_filmworks(self) -> list[SQLitePersonFilmWork]:
        """Extract person_filmworks from SQLite"""
        return
