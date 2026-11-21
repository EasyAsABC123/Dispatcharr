CREATE TABLE "new__vod_movie" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "uuid" char(32) NOT NULL UNIQUE, "name" varchar(255) NOT NULL, "description" text NULL, "year" integer NULL, "rating" varchar(10) NULL, "genre" varchar(255) NULL, "duration_secs" integer NULL, "tmdb_id" varchar(50) NULL UNIQUE, "imdb_id" varchar(50) NULL UNIQUE, "custom_properties" text NULL CHECK ((JSON_VALID("custom_properties") OR "custom_properties" IS NULL)), "created_at" datetime NOT NULL, "updated_at" datetime NOT NULL, "logo_id" bigint NULL REFERENCES "vod_vodlogo" ("id") DEFERRABLE INITIALLY DEFERRED, "is_adult" bool NOT NULL)
INSERT INTO "new__vod_movie" ("id", "uuid", "name", "description", "year", "rating", "genre", "duration_secs", "tmdb_id", "imdb_id", "custom_properties", "created_at", "updated_at", "logo_id", is_adult) SELECT "id", "uuid", "name", "description", "year", "rating", "genre", "duration_secs", "tmdb_id", "imdb_id", "custom_properties", "created_at", "updated_at", "logo_id", 0 FROM "vod_movie"
DROP TABLE "vod_movie"
ALTER TABLE "new__vod_movie" RENAME TO "vod_movie"
CREATE UNIQUE INDEX "unique_movie_name_year_no_external_id" ON "vod_movie" ("name", "year") WHERE ("tmdb_id" IS NULL AND "imdb_id" IS NULL)
CREATE INDEX "vod_movie_is_adult_a63dfa81" ON "vod_movie" ("is_adult")
CREATE INDEX "vod_movie_logo_id_c9c89b1b" ON "vod_movie" ("logo_id")
