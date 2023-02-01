CREATE SCHEMA IF NOT EXISTS content;

CREATE TABLE IF NOT EXISTS content.film_work (
    id uuid PRIMARY KEY,
    title TEXT NOT NULL,
    description TEXT,
    creation_date DATE,
    rating FLOAT,
    type TEXT NOT NULL,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    modified TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS content.genre (
    id uuid PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    modified TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS content.genre_film_work (
    id uuid PRIMARY KEY,
    genre_id uuid NOT NULL REFERENCES content.genre (id) ON DELETE CASCADE,
    film_work_id uuid NOT NULL REFERENCES content.film_work (id) ON DELETE CASCADE,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS content.person (
    id uuid PRIMARY KEY,
    full_name TEXT NOT NULL,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    modified TIMESTAMP WITH TIME ZONE
);

CREATE TABLE IF NOT EXISTS content.person_film_work (
    id uuid PRIMARY KEY,
    person_id uuid NOT NULL REFERENCES content.person (id) ON DELETE CASCADE,
    film_work_id uuid NOT NULL REFERENCES content.film_work (id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    created TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX genre_film_work_idx ON content.genre_film_work (genre_id, film_work_id);
CREATE INDEX film_work_creation_rate_idx ON content.film_work (creation_date, rating);
CREATE UNIQUE INDEX person_film_work_idx ON content.person_film_work (person_id, film_work_id, role);
