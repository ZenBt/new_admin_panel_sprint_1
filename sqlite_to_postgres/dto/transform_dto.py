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

    def __bool__(self):
        return any(
            (
                self.film_works,
                self.genres,
                self.persons,
            )
        )


@dataclass
class RelationalSQLiteMovies:
    genre_film_works: list[SQLiteGenreFilmwork]
    person_film_works: list[SQLitePersonFilmWork]

    def __bool__(self):
        return any(
            (
                self.genre_film_works,
                self.person_film_works,
            )
        )


@dataclass
class PGMovies:
    filmworks: list[PGFilmWork]
    genres: list[PGGenre]
    persons: list[PGPerson]


@dataclass
class RelationalPGMovies:
    genre_film_works: list[PGGenreFilmwork]
    person_film_works: list[PGPersonFilmWork]

