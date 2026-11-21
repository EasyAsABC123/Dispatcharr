CREATE TABLE "new__epg_epgdata" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "tvg_id" varchar(255) NULL, "epg_source_id" bigint NULL REFERENCES "epg_epgsource" ("id") DEFERRABLE INITIALLY DEFERRED, "icon_url" varchar(500) NULL, "name" varchar(512) NOT NULL)
INSERT INTO "new__epg_epgdata" ("id", "name", "epg_source_id", "tvg_id", "icon_url") SELECT "id", "name", "epg_source_id", "tvg_id", "icon_url" FROM "epg_epgdata"
DROP TABLE "epg_epgdata"
ALTER TABLE "new__epg_epgdata" RENAME TO "epg_epgdata"
CREATE INDEX "epg_epgdata_epg_source_id_194a02dc" ON "epg_epgdata" ("epg_source_id")
CREATE INDEX "epg_epgdata_tvg_id_228d3b9f" ON "epg_epgdata" ("tvg_id")
CREATE UNIQUE INDEX "epg_epgdata_tvg_id_epg_source_id_d0a49d70_uniq" ON "epg_epgdata" ("tvg_id", "epg_source_id")
