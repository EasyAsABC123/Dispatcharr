ALTER TABLE "epg_epgsource" ADD COLUMN "programme_index" text NULL CHECK ((JSON_VALID("programme_index") OR "programme_index" IS NULL))
