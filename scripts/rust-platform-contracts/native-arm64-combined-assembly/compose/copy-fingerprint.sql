WITH migrations AS (
  SELECT jsonb_agg(jsonb_build_array(id, app, name, applied) ORDER BY id) AS rows
  FROM django_migrations
), actors AS (
  SELECT jsonb_agg(jsonb_build_array(id, username, is_superuser, is_staff, is_active, user_level) ORDER BY id) AS rows
  FROM accounts_user WHERE username = 'owned-arm64-admin'
), channels AS (
  SELECT jsonb_agg(jsonb_build_array(c.id, c.uuid, c.channel_number, c.name, c.stream_profile_id, cs."order", s.id, s.stream_hash)
    ORDER BY c.id, cs."order", s.id) AS rows
  FROM dispatcharr_channels_channel c
  JOIN dispatcharr_channels_channelstream cs ON cs.channel_id = c.id
  JOIN dispatcharr_channels_stream s ON s.id = cs.stream_id
  WHERE c.uuid = '3b81d384-d6a1-4d64-938e-3e91075cec0a'
)
SELECT jsonb_build_object(
  'migrations', jsonb_array_length(migrations.rows), 'actors', jsonb_array_length(actors.rows),
  'channel_streams', jsonb_array_length(channels.rows),
  'fingerprint', md5(jsonb_build_array(migrations.rows, actors.rows, channels.rows)::text)
)
FROM migrations, actors, channels;
