from dataclasses import dataclass

from .pg_dto import (
    FilmWork as PGFilmWork,
    Person as PGPerson,
    Genre as PGGenre,
    GenreFilmwork as PGGenreFilmwork,
    PersonFilmWork as PGPersonFilmWork,
)
from .sqlite_dto import (
    FilmWork as SQLiteFilmWork,
    Person as SQLitePerson,
    Genre as SQLiteGenre,
    GenreFilmwork as SQLiteGenreFilmwork,
    PersonFilmWork as SQLitePersonFilmWork,
)


@dataclass
class SQLiteMovies:
    film_works: list[SQLiteFilmWork]
    genres: list[SQLiteGenre]
    persons: list[SQLitePerson]
    genre_filmworks: list[SQLiteGenreFilmwork]
    person_filmworks: list[SQLitePersonFilmWork]

    def __bool__(self):
        return any(
            (
                self.film_works,
                self.genres,
                self.persons,
                self.genre_filmworks,
                self.person_filmworks,
            )
        )


@dataclass
class PGMovies:
    filmworks: list[PGFilmWork]
    genres: list[PGGenre]
    persons: list[PGPerson]
    genre_filmworks: list[PGGenreFilmwork]
    person_filmworks: list[PGPersonFilmWork]
