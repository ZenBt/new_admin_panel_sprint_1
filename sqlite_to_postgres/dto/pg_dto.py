from dataclasses import dataclass
from uuid import UUID
from datetime import datetime, date


@dataclass
class UUIDMixin:
    id: UUID


@dataclass
class TimeStampedMixin:
    created: datetime | None
    modified: datetime | None


@dataclass
class FilmWork(UUIDMixin, TimeStampedMixin):
    title: str
    file_path: str | None
    description: str | None
    creation_date: date | None
    rating: float | None
    type: str


@dataclass
class Person(UUIDMixin, TimeStampedMixin):
    full_name: str


@dataclass
class Genre(UUIDMixin, TimeStampedMixin):
    name: str
    description: str | None


@dataclass
class GenreFilmwork(UUIDMixin):
    film_work_id: UUID
    genre_id: UUID
    created: datetime | None


@dataclass
class PersonFilmWork(UUIDMixin):
    film_work_id: UUID
    person_id: UUID
    role: str
    created: datetime | None
