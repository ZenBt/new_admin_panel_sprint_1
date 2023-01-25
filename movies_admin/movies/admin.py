from django.contrib import admin
from .models import Genre, FilmWork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    pass 


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    pass
