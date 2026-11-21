ALTER TABLE "vod_m3uepisoderelation" ADD COLUMN "series_relation_id" bigint NULL REFERENCES "vod_m3useriesrelation" ("id") DEFERRABLE INITIALLY DEFERRED
CREATE INDEX "vod_m3uepisoderelation_series_relation_id_c2ff56f2" ON "vod_m3uepisoderelation" ("series_relation_id")
