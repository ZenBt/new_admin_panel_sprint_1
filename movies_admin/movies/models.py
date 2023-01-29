import uuid
from django.db import models
from django.core.validators import MinValueValidator, MaxValueValidator
from django.utils.translation import gettext_lazy as _

from movies.choices import FilmWorkType, RoleChoices


class TimeStampedMixin(models.Model):
    
    created = models.DateTimeField(verbose_name=_('Created at'), auto_now_add=True)
    modified = models.DateTimeField(verbose_name=_('Modified at'), auto_now=True)

    class Meta:

        abstract = True


class UUIDMixin(models.Model):

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:

        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):

    name = models.CharField(verbose_name=_("Name"), max_length=255)
    description = models.TextField(verbose_name=_("Description"), blank=True)

    class Meta:

        db_table = "content\".\"genre"

        verbose_name = _("Genre")
        verbose_name_plural = _("Genres")
    

    def __str__(self):
        return self.name 


class FilmWork(UUIDMixin, TimeStampedMixin):

    title = models.CharField(verbose_name=_("Title"), max_length=255)
    file_path = models.TextField(verbose_name=_("File path"), blank=True, null=True)
    description = models.TextField(verbose_name=_("Description"), blank=True)
    creation_date = models.DateField(verbose_name=_("Creation date"))
    rating = models.FloatField(
        verbose_name=_("Rating"),
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(10)]
    )
    type = models.CharField(verbose_name=_("Type"), max_length=255, choices=FilmWorkType.choices)
    genres = models.ManyToManyField(Genre, through='GenreFilmwork')

    class Meta:

        db_table = "content\".\"film_work"

        verbose_name = _("Filmwork")
        verbose_name_plural = _("Filmworks")


    def __str__(self):
        return self.title 


class GenreFilmwork(UUIDMixin):

    film_work = models.ForeignKey(FilmWork, on_delete=models.CASCADE)
    genre = models.ForeignKey(Genre, on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:

        db_table = "content\".\"genre_film_work" 



class Person(UUIDMixin, TimeStampedMixin):
    
    full_name = models.CharField(verbose_name=_("Full name"), max_length=255)
    film_works = models.ManyToManyField(FilmWork, through='PersonFilmwork')   
    
    class Meta:

        db_table = "content\".\"person"

        verbose_name = _("Person")
        verbose_name_plural = _("Persons")
    
    def __str__(self):
        return self.full_name


class PersonFilmwork(UUIDMixin):

    film_work = models.ForeignKey(FilmWork, on_delete=models.CASCADE)
    person = models.ForeignKey(Person, on_delete=models.CASCADE)
    role = models.TextField(verbose_name=_('Role'), null=True, choices=RoleChoices.choices)
    created = models.DateTimeField(verbose_name=_('Created at'), auto_now_add=True)

    class Meta:

        db_table = "content\".\"person_film_work"

        verbose_name = _("Person filmwork")
        verbose_name_plural = _("Person filmworks")
    