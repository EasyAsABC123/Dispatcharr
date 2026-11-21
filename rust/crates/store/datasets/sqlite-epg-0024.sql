ALTER TABLE "epg_epgsource" DROP COLUMN "api_key"
ALTER TABLE "epg_epgsource" ADD COLUMN "password" varchar(255) NULL
ALTER TABLE "epg_epgsource" ADD COLUMN "username" varchar(255) NULL
ALTER TABLE "epg_programdata" ADD COLUMN "program_id" varchar(64) NULL
CREATE TABLE "epg_sdprogrammd5" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "program_id" varchar(64) NOT NULL, "md5" varchar(22) NOT NULL, "epg_source_id" bigint NOT NULL REFERENCES "epg_epgsource" ("id") DEFERRABLE INITIALLY DEFERRED)
CREATE INDEX "epg_sdprogrammd5_epg_source_id_7bb64712" ON "epg_sdprogrammd5" ("epg_source_id")
CREATE UNIQUE INDEX "epg_sdprogrammd5_epg_source_id_program_id_2c568d9f_uniq" ON "epg_sdprogrammd5" ("epg_source_id", "program_id")
CREATE TABLE "epg_sdschedulemd5" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "station_id" varchar(20) NOT NULL, "date" date NOT NULL, "md5" varchar(22) NOT NULL, "last_modified" datetime NOT NULL, "epg_source_id" bigint NOT NULL REFERENCES "epg_epgsource" ("id") DEFERRABLE INITIALLY DEFERRED)
CREATE INDEX "epg_sdsched_epg_sou_0d700e_idx" ON "epg_sdschedulemd5" ("epg_source_id", "station_id")
CREATE INDEX "epg_sdschedulemd5_epg_source_id_4f75c39f" ON "epg_sdschedulemd5" ("epg_source_id")
CREATE UNIQUE INDEX "epg_sdschedulemd5_epg_source_id_station_id_date_2fef8348_uniq" ON "epg_sdschedulemd5" ("epg_source_id", "station_id", "date")
