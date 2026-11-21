CREATE TABLE "new__vod_movie" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "uuid" char(32) NOT NULL UNIQUE, "name" varchar(255) NOT NULL, "description" text NULL, "year" integer NULL, "rating" varchar(10) NULL, "genre" varchar(255) NULL, "duration_secs" integer NULL, "imdb_id" varchar(50) NULL, "custom_properties" text NULL CHECK ((JSON_VALID("custom_properties") OR "custom_properties" IS NULL)), "created_at" datetime NOT NULL, "updated_at" datetime NOT NULL, "logo_id" bigint NULL REFERENCES "vod_vodlogo" ("id") DEFERRABLE INITIALLY DEFERRED, "is_adult" bool NOT NULL, "language" varchar(2) NOT NULL, "tmdb_id" varchar(50) NULL)
INSERT INTO "new__vod_movie" ("id", "uuid", "name", "description", "year", "rating", "genre", "duration_secs", "tmdb_id", "imdb_id", "custom_properties", "created_at", "updated_at", "logo_id", "is_adult", language) SELECT "id", "uuid", "name", "description", "year", "rating", "genre", "duration_secs", "tmdb_id", "imdb_id", "custom_properties", "created_at", "updated_at", "logo_id", "is_adult", '' FROM "vod_movie"
DROP TABLE "vod_movie"
ALTER TABLE "new__vod_movie" RENAME TO "vod_movie"
CREATE UNIQUE INDEX "unique_movie_imdb_id_language" ON "vod_movie" ("imdb_id", "language") WHERE "imdb_id" IS NOT NULL
CREATE UNIQUE INDEX "unique_movie_name_year_language_no_external_id" ON "vod_movie" ("name", "year", "language") WHERE ("tmdb_id" IS NULL AND "imdb_id" IS NULL)
CREATE UNIQUE INDEX "unique_movie_tmdb_id_language" ON "vod_movie" ("tmdb_id", "language") WHERE "tmdb_id" IS NOT NULL
CREATE INDEX "vod_movie_imdb_id_cf11b779" ON "vod_movie" ("imdb_id")
CREATE INDEX "vod_movie_is_adult_a63dfa81" ON "vod_movie" ("is_adult")
CREATE INDEX "vod_movie_logo_id_c9c89b1b" ON "vod_movie" ("logo_id")
CREATE INDEX "vod_movie_tmdb_id_89bf1a57" ON "vod_movie" ("tmdb_id")
CREATE TABLE "new__vod_series" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "uuid" char(32) NOT NULL UNIQUE, "name" varchar(255) NOT NULL, "description" text NULL, "year" integer NULL, "rating" varchar(10) NULL, "genre" varchar(255) NULL, "imdb_id" varchar(50) NULL, "custom_properties" text NULL CHECK ((JSON_VALID("custom_properties") OR "custom_properties" IS NULL)), "created_at" datetime NOT NULL, "updated_at" datetime NOT NULL, "logo_id" bigint NULL REFERENCES "vod_vodlogo" ("id") DEFERRABLE INITIALLY DEFERRED, "language" varchar(2) NOT NULL, "tmdb_id" varchar(50) NULL)
INSERT INTO "new__vod_series" ("id", "uuid", "name", "description", "year", "rating", "genre", "tmdb_id", "imdb_id", "custom_properties", "created_at", "updated_at", "logo_id", language) SELECT "id", "uuid", "name", "description", "year", "rating", "genre", "tmdb_id", "imdb_id", "custom_properties", "created_at", "updated_at", "logo_id", '' FROM "vod_series"
DROP TABLE "vod_series"
ALTER TABLE "new__vod_series" RENAME TO "vod_series"
CREATE UNIQUE INDEX "unique_series_imdb_id_language" ON "vod_series" ("imdb_id", "language") WHERE "imdb_id" IS NOT NULL
CREATE UNIQUE INDEX "unique_series_name_year_language_no_external_id" ON "vod_series" ("name", "year", "language") WHERE ("tmdb_id" IS NULL AND "imdb_id" IS NULL)
CREATE UNIQUE INDEX "unique_series_tmdb_id_language" ON "vod_series" ("tmdb_id", "language") WHERE "tmdb_id" IS NOT NULL
CREATE INDEX "vod_series_imdb_id_82c8f673" ON "vod_series" ("imdb_id")
CREATE INDEX "vod_series_logo_id_9edbac2d" ON "vod_series" ("logo_id")
CREATE INDEX "vod_series_tmdb_id_6872ed6c" ON "vod_series" ("tmdb_id")
