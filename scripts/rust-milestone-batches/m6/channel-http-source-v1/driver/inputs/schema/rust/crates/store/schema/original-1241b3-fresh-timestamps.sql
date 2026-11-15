-- Derived from original Dispatcharr contributors and Django migrations.
-- Original baseline: 1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430; see ATTRIBUTION.md.
-- Fresh-database initialization only; never use to alter an existing schema.
UPDATE public."core_systemnotification" SET "created_at"=CURRENT_TIMESTAMP WHERE "created_at" IS NOT NULL;
UPDATE public."core_systemnotification" SET "updated_at"=CURRENT_TIMESTAMP WHERE "updated_at" IS NOT NULL;
UPDATE public."core_useragent" SET "created_at"=CURRENT_TIMESTAMP WHERE "created_at" IS NOT NULL;
UPDATE public."core_useragent" SET "updated_at"=CURRENT_TIMESTAMP WHERE "updated_at" IS NOT NULL;
UPDATE public."django_celery_beat_periodictask" SET "date_changed"=CURRENT_TIMESTAMP WHERE "date_changed" IS NOT NULL;
UPDATE public."django_celery_beat_periodictasks" SET "last_update"=CURRENT_TIMESTAMP WHERE "last_update" IS NOT NULL;
UPDATE public."django_migrations" SET "applied"=CURRENT_TIMESTAMP WHERE "applied" IS NOT NULL;
UPDATE public."m3u_m3uaccount" SET "created_at"=CURRENT_TIMESTAMP WHERE "created_at" IS NOT NULL;
UPDATE public."m3u_m3uaccount" SET "updated_at"=CURRENT_TIMESTAMP WHERE "updated_at" IS NOT NULL;
UPDATE public."plugins_pluginrepo" SET "created_at"=CURRENT_TIMESTAMP WHERE "created_at" IS NOT NULL;
UPDATE public."plugins_pluginrepo" SET "updated_at"=CURRENT_TIMESTAMP WHERE "updated_at" IS NOT NULL;
