from django.db.models import TextChoices


class FilmWorkType(TextChoices):

    MOVIE = "movie", "Фильм"
    SHOW = "tv_show", "Сериал"


class RoleChoices(TextChoices):

    ACTOR = "actor", "Актер"
    DIRECTOR = "director", "Режиссер"
    WRITER = "writer", "Сценарист"
