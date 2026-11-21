BEGIN;
INSERT INTO m3u_m3uaccount
(name,max_streams,is_active,created_at,locked,refresh_interval,stale_stream_days,account_type,status,priority)
VALUES ('owned ARM64 media fixture',1,true,now(),false,0,7,'STD','success',0);
INSERT INTO m3u_m3uaccountprofile
(name,is_default,max_streams,is_active,search_pattern,replace_pattern,current_viewers,m3u_account_id)
VALUES ('owned ARM64 slot',true,1,true,'','',0,
 (SELECT id FROM m3u_m3uaccount WHERE name='owned ARM64 media fixture'));
INSERT INTO dispatcharr_channels_stream
(name,url,current_viewers,updated_at,m3u_account_id,is_custom,last_seen,stream_hash,is_stale,is_adult,is_catchup,catchup_days,is_radio)
VALUES ('owned ARM64 stream','http://media-fixture:8080/media.ts',0,now(),
 (SELECT id FROM m3u_m3uaccount WHERE name='owned ARM64 media fixture'),true,now(),'owned-arm64-stream',false,false,false,0,false);
INSERT INTO dispatcharr_channels_channel
(channel_number,name,stream_profile_id,uuid,user_level,auto_created,created_at,updated_at,is_adult,hidden_from_output,is_catchup,catchup_days,is_radio)
VALUES (9001,'owned ARM64 channel',3,'3b81d384-d6a1-4d64-938e-3e91075cec0a',0,false,now(),now(),false,false,false,0,false);
INSERT INTO dispatcharr_channels_channelstream ("order",channel_id,stream_id)
SELECT 0,c.id,s.id FROM dispatcharr_channels_channel c CROSS JOIN dispatcharr_channels_stream s
WHERE c.uuid='3b81d384-d6a1-4d64-938e-3e91075cec0a' AND s.stream_hash='owned-arm64-stream';
COMMIT;
