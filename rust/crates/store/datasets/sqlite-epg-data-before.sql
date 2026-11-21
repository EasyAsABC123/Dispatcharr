CREATE TABLE "epg_epgdata" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "name" varchar(255) NOT NULL, "epg_source_id" bigint NULL REFERENCES "epg_epgsource" ("id") DEFERRABLE INITIALLY DEFERRED, "tvg_id" varchar(255) NULL, "icon_url" varchar(500) NULL)
CREATE INDEX "epg_epgdata_epg_source_id_194a02dc" ON "epg_epgdata" ("epg_source_id")
CREATE INDEX "epg_epgdata_tvg_id_228d3b9f" ON "epg_epgdata" ("tvg_id")
CREATE UNIQUE INDEX "epg_epgdata_tvg_id_epg_source_id_d0a49d70_uniq" ON "epg_epgdata" ("tvg_id", "epg_source_id")
