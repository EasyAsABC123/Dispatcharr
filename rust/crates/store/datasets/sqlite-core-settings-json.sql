CREATE TABLE "new__core_coresettings" ("id" integer NOT NULL PRIMARY KEY AUTOINCREMENT, "value" text NOT NULL CHECK ((JSON_VALID("value") OR "value" IS NULL)), "key" varchar(255) NOT NULL UNIQUE, "name" varchar(255) NOT NULL)
INSERT INTO "new__core_coresettings"(id,key,name,value) SELECT id,key,name,value FROM "core_coresettings"
DROP TABLE "core_coresettings"
ALTER TABLE "new__core_coresettings" RENAME TO "core_coresettings"
