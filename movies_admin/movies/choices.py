from django.db.models import TextChoices


class FilmWorkType(TextChoices):

    MOVIE = "movie", "Фильм"
    SHOW = "tv_show", "Сериал"
