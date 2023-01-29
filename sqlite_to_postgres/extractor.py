from sqlite3 import Connection as SQLiteConnection, Row

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
        self._chunk_size = 1000
        self._conn = connection
        self._is_selected = False
        self._set_row_factory()
        self._init_cursors()
    
    def _init_cursors(self) -> None:
        self._p_cursor = self._conn.cursor()
        self._f_cursor = self._conn.cursor()
        self._g_cursor = self._conn.cursor()
        self._gf_cursor = self._conn.cursor()
        self._pf_cursor = self._conn.cursor()
    
    def _set_row_factory(self) -> None:
        self._conn.row_factory = Row

    def set_chunk_size(self, chunk_size: int) -> None:
        self._chunk_size = chunk_size

    def extract_movies(self, chunk_size: int) -> SQLiteMovies:
        """Extract movies from SQLite"""
        if not self._is_selected:
            self._make_selects()
            self._is_selected = True
        
        film_works = self._extract_filmworks()
        genres = self._extract_genres()
        persons = self._extract_persons()
        genre_filmworks = self._extract_genre_filmworks()
        person_filmworks = self._extract_person_filmworks()

        return self._make_sqlite_data(
            film_works, genres, persons, genre_filmworks, person_filmworks
        )
    
    def _make_selects(self) -> None:
        self._p_cursor.execute("SELECT * FROM person;")
        self._f_cursor.execute("SELECT * FROM film_work;")
        self._g_cursor.execute("SELECT * FROM genre;")
        self._gf_cursor.execute("SELECT * FROM genre_film_work;")
        self._pf_cursor.execute("SELECT * FROM person_film_work;")

    def _extract_persons(self) -> list[SQLitePerson]:
        """Extract persons from SQLite"""
        res = self._p_cursor.fetchmany(self._chunk_size)
        return [SQLitePerson(**row) for row in res]


    def _extract_filmworks(self) -> list[SQLiteFilmWork]:
        """Extract filmworks from SQLite"""
        res = self._f_cursor.fetchmany(self._chunk_size)
        return [SQLiteFilmWork(**row) for row in res]

    def _extract_genres(self) -> list[SQLiteGenre]:
        """Extract genres from SQLite"""
        res = self._g_cursor.fetchmany(self._chunk_size)
        return [SQLiteGenre(**row) for row in res]

    def _extract_genre_filmworks(self) -> list[SQLiteGenreFilmwork]:
        """Extract genre_filmworks from SQLite"""
        res = self._gf_cursor.fetchmany(self._chunk_size)
        return [SQLiteGenreFilmwork(**row) for row in res]


    def _extract_person_filmworks(self) -> list[SQLitePersonFilmWork]:
        """Extract person_filmworks from SQLite"""
        res = self._pf_cursor.fetchmany(self._chunk_size)
        return [SQLitePersonFilmWork(**row) for row in res]

    def _make_sqlite_data(
        self, film_works, genres, persons, genre_filmworks, person_filmworks
    ):
        """Make SQLiteMovies object"""
        return SQLiteMovies(
            film_works=film_works,
            genres=genres,
            persons=persons,
            genre_filmworks=genre_filmworks,
            person_filmworks=person_filmworks,
        )
