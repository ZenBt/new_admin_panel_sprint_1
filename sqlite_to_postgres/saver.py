from psycopg2.extensions import connection as _connection

from .dto import (
    PGMovies,
    PGFilmWork,
    PGPerson,
    PGGenre,
    PGGenreFilmwork,
    PGPersonFilmWork,
)


class PostgresSaver:
    def __init__(self, connection: _connection):
        self._conn = connection
    
    def save_all_data(self, data: PGMovies):
        self._save_filmworks(data.filmworks)
        self._save_genres(data.genres)
        self._save_persons(data.persons)
        self._save_genre_filmworks(data.genre_filmworks)
        self._save_person_filmworks(data.person_filmworks)
    
    def _save_filmworks(self, filmworks: list[PGFilmWork]):
        pass
    
    def _save_genres(self, genres: list[PGGenre]):
        pass
    
    def _save_persons(self, persons: list[PGPerson]):
        pass

    def _save_genre_filmworks(self, genre_filmworks: list[PGGenreFilmwork]):
        pass

    def _save_person_filmworks(self, person_filmworks: list[PGPersonFilmWork]):
        pass
