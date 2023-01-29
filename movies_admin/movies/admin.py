from django.contrib import admin
from .models import Genre, FilmWork, GenreFilmwork, Person, PersonFilmwork


class GenreFilmworkInline(admin.TabularInline):
    model = GenreFilmwork
    autocomplete_fields = ('genre',) 


class PersonFilmworkInline(admin.TabularInline):
    model = PersonFilmwork
    autocomplete_fields = ('film_work',)


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    inlines = (PersonFilmworkInline,)

    list_display = ("full_name", "created", "modified")
    search_fields = ("full_name", "id")


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    
    list_display = ("name", "created", "modified")
    search_fields = ("name", "id")


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmworkInline,)

    list_display = ("title", "type", "creation_date", "rating", "created", "modified")
    list_filter = ("type",)
    search_fields = ("title", "description", "id")
