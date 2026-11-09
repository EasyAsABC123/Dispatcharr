-- Derived from original Dispatcharr contributors and Django migrations.
-- Original baseline: 1241b3f7ffb2e64c8d1157ecd5e3b199af1ae430; see ATTRIBUTION.md.
-- Fresh-database initialization only; never use to alter an existing schema.
--
-- PostgreSQL database dump
--


-- Dumped from database version 17.11 (Ubuntu 17.11-1.pgdg26.04+2)
-- Dumped by pg_dump version 17.11 (Ubuntu 17.11-1.pgdg26.04+2)


--
-- Data for Name: accounts_user; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: dispatcharr_channels_channelprofile; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: accounts_user_channel_profiles; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: auth_group; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: accounts_user_groups; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: django_content_type; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.django_content_type (id, app_label, model) VALUES (1, 'accounts', 'user');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (2, 'accounts', 'userappdata');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (3, 'dispatcharr_channels', 'channel');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (4, 'dispatcharr_channels', 'channelgroup');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (5, 'dispatcharr_channels', 'channelgroupm3uaccount');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (6, 'dispatcharr_channels', 'channeloverride');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (7, 'dispatcharr_channels', 'channelprofile');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (8, 'dispatcharr_channels', 'channelprofilemembership');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (9, 'dispatcharr_channels', 'channelstream');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (10, 'dispatcharr_channels', 'logo');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (11, 'dispatcharr_channels', 'recording');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (12, 'dispatcharr_channels', 'recurringrecordingrule');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (13, 'dispatcharr_channels', 'stream');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (14, 'dashboard', 'settings');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (15, 'epg', 'epgdata');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (16, 'epg', 'epgsource');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (17, 'epg', 'epgsourceindex');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (18, 'epg', 'programdata');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (19, 'epg', 'sdprogrammd5');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (20, 'epg', 'sdschedulemd5');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (21, 'hdhr', 'hdhrdevice');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (22, 'm3u', 'm3uaccount');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (23, 'm3u', 'm3uaccountprofile');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (24, 'm3u', 'm3ufilter');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (25, 'm3u', 'servergroup');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (26, 'vod', 'episode');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (27, 'vod', 'm3uepisoderelation');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (28, 'vod', 'm3umovierelation');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (29, 'vod', 'm3useriesrelation');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (30, 'vod', 'm3uvodcategoryrelation');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (31, 'vod', 'movie');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (32, 'vod', 'series');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (33, 'vod', 'vodcategory');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (34, 'vod', 'vodlogo');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (35, 'dispatcharr_connect', 'deliverylog');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (36, 'dispatcharr_connect', 'eventsubscription');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (37, 'dispatcharr_connect', 'integration');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (38, 'core', 'coresettings');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (39, 'core', 'notificationdismissal');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (40, 'core', 'outputprofile');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (41, 'core', 'streamprofile');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (42, 'core', 'systemevent');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (43, 'core', 'systemnotification');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (44, 'core', 'useragent');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (45, 'admin', 'logentry');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (46, 'auth', 'group');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (47, 'auth', 'permission');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (48, 'contenttypes', 'contenttype');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (49, 'sessions', 'session');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (50, 'django_celery_beat', 'clockedschedule');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (51, 'django_celery_beat', 'crontabschedule');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (52, 'django_celery_beat', 'intervalschedule');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (53, 'django_celery_beat', 'periodictask');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (54, 'django_celery_beat', 'periodictasks');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (55, 'django_celery_beat', 'solarschedule');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (56, 'plugins', 'pluginconfig');
INSERT INTO public.django_content_type (id, app_label, model) VALUES (57, 'plugins', 'pluginrepo');


--
-- Data for Name: auth_permission; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (1, 'Can add user', 1, 'add_user');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (2, 'Can change user', 1, 'change_user');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (3, 'Can delete user', 1, 'delete_user');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (4, 'Can view user', 1, 'view_user');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (5, 'Can add user app data', 2, 'add_userappdata');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (6, 'Can change user app data', 2, 'change_userappdata');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (7, 'Can delete user app data', 2, 'delete_userappdata');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (8, 'Can view user app data', 2, 'view_userappdata');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (9, 'Can add channel group', 4, 'add_channelgroup');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (10, 'Can change channel group', 4, 'change_channelgroup');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (11, 'Can delete channel group', 4, 'delete_channelgroup');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (12, 'Can view channel group', 4, 'view_channelgroup');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (13, 'Can add channel', 3, 'add_channel');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (14, 'Can change channel', 3, 'change_channel');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (15, 'Can delete channel', 3, 'delete_channel');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (16, 'Can view channel', 3, 'view_channel');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (17, 'Can add Stream', 13, 'add_stream');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (18, 'Can change Stream', 13, 'change_stream');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (19, 'Can delete Stream', 13, 'delete_stream');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (20, 'Can view Stream', 13, 'view_stream');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (21, 'Can add channel stream', 9, 'add_channelstream');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (22, 'Can change channel stream', 9, 'change_channelstream');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (23, 'Can delete channel stream', 9, 'delete_channelstream');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (24, 'Can view channel stream', 9, 'view_channelstream');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (25, 'Can add channel group m3u account', 5, 'add_channelgroupm3uaccount');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (26, 'Can change channel group m3u account', 5, 'change_channelgroupm3uaccount');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (27, 'Can delete channel group m3u account', 5, 'delete_channelgroupm3uaccount');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (28, 'Can view channel group m3u account', 5, 'view_channelgroupm3uaccount');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (29, 'Can add logo', 10, 'add_logo');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (30, 'Can change logo', 10, 'change_logo');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (31, 'Can delete logo', 10, 'delete_logo');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (32, 'Can view logo', 10, 'view_logo');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (33, 'Can add channel profile', 7, 'add_channelprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (34, 'Can change channel profile', 7, 'change_channelprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (35, 'Can delete channel profile', 7, 'delete_channelprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (36, 'Can view channel profile', 7, 'view_channelprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (37, 'Can add channel profile membership', 8, 'add_channelprofilemembership');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (38, 'Can change channel profile membership', 8, 'change_channelprofilemembership');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (39, 'Can delete channel profile membership', 8, 'delete_channelprofilemembership');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (40, 'Can view channel profile membership', 8, 'view_channelprofilemembership');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (41, 'Can add recording', 11, 'add_recording');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (42, 'Can change recording', 11, 'change_recording');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (43, 'Can delete recording', 11, 'delete_recording');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (44, 'Can view recording', 11, 'view_recording');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (45, 'Can add recurring recording rule', 12, 'add_recurringrecordingrule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (46, 'Can change recurring recording rule', 12, 'change_recurringrecordingrule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (47, 'Can delete recurring recording rule', 12, 'delete_recurringrecordingrule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (48, 'Can view recurring recording rule', 12, 'view_recurringrecordingrule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (49, 'Can add channel override', 6, 'add_channeloverride');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (50, 'Can change channel override', 6, 'change_channeloverride');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (51, 'Can delete channel override', 6, 'delete_channeloverride');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (52, 'Can view channel override', 6, 'view_channeloverride');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (53, 'Can add settings', 14, 'add_settings');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (54, 'Can change settings', 14, 'change_settings');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (55, 'Can delete settings', 14, 'delete_settings');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (56, 'Can view settings', 14, 'view_settings');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (57, 'Can add epg data', 15, 'add_epgdata');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (58, 'Can change epg data', 15, 'change_epgdata');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (59, 'Can delete epg data', 15, 'delete_epgdata');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (60, 'Can view epg data', 15, 'view_epgdata');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (61, 'Can add epg source', 16, 'add_epgsource');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (62, 'Can change epg source', 16, 'change_epgsource');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (63, 'Can delete epg source', 16, 'delete_epgsource');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (64, 'Can view epg source', 16, 'view_epgsource');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (65, 'Can add program data', 18, 'add_programdata');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (66, 'Can change program data', 18, 'change_programdata');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (67, 'Can delete program data', 18, 'delete_programdata');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (68, 'Can view program data', 18, 'view_programdata');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (69, 'Can add sd program m d5', 19, 'add_sdprogrammd5');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (70, 'Can change sd program m d5', 19, 'change_sdprogrammd5');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (71, 'Can delete sd program m d5', 19, 'delete_sdprogrammd5');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (72, 'Can view sd program m d5', 19, 'view_sdprogrammd5');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (73, 'Can add sd schedule m d5', 20, 'add_sdschedulemd5');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (74, 'Can change sd schedule m d5', 20, 'change_sdschedulemd5');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (75, 'Can delete sd schedule m d5', 20, 'delete_sdschedulemd5');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (76, 'Can view sd schedule m d5', 20, 'view_sdschedulemd5');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (77, 'Can add epg source index', 17, 'add_epgsourceindex');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (78, 'Can change epg source index', 17, 'change_epgsourceindex');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (79, 'Can delete epg source index', 17, 'delete_epgsourceindex');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (80, 'Can view epg source index', 17, 'view_epgsourceindex');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (81, 'Can add hdhr device', 21, 'add_hdhrdevice');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (82, 'Can change hdhr device', 21, 'change_hdhrdevice');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (83, 'Can delete hdhr device', 21, 'delete_hdhrdevice');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (84, 'Can view hdhr device', 21, 'view_hdhrdevice');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (85, 'Can add server group', 25, 'add_servergroup');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (86, 'Can change server group', 25, 'change_servergroup');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (87, 'Can delete server group', 25, 'delete_servergroup');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (88, 'Can view server group', 25, 'view_servergroup');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (89, 'Can add m3u account', 22, 'add_m3uaccount');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (90, 'Can change m3u account', 22, 'change_m3uaccount');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (91, 'Can delete m3u account', 22, 'delete_m3uaccount');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (92, 'Can view m3u account', 22, 'view_m3uaccount');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (93, 'Can add m3u filter', 24, 'add_m3ufilter');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (94, 'Can change m3u filter', 24, 'change_m3ufilter');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (95, 'Can delete m3u filter', 24, 'delete_m3ufilter');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (96, 'Can view m3u filter', 24, 'view_m3ufilter');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (97, 'Can add m3u account profile', 23, 'add_m3uaccountprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (98, 'Can change m3u account profile', 23, 'change_m3uaccountprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (99, 'Can delete m3u account profile', 23, 'delete_m3uaccountprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (100, 'Can view m3u account profile', 23, 'view_m3uaccountprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (101, 'Can add Movie', 31, 'add_movie');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (102, 'Can change Movie', 31, 'change_movie');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (103, 'Can delete Movie', 31, 'delete_movie');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (104, 'Can view Movie', 31, 'view_movie');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (105, 'Can add Series', 32, 'add_series');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (106, 'Can change Series', 32, 'change_series');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (107, 'Can delete Series', 32, 'delete_series');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (108, 'Can view Series', 32, 'view_series');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (109, 'Can add Episode', 26, 'add_episode');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (110, 'Can change Episode', 26, 'change_episode');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (111, 'Can delete Episode', 26, 'delete_episode');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (112, 'Can view Episode', 26, 'view_episode');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (113, 'Can add VOD Category', 33, 'add_vodcategory');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (114, 'Can change VOD Category', 33, 'change_vodcategory');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (115, 'Can delete VOD Category', 33, 'delete_vodcategory');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (116, 'Can view VOD Category', 33, 'view_vodcategory');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (117, 'Can add M3U VOD Category Relation', 30, 'add_m3uvodcategoryrelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (118, 'Can change M3U VOD Category Relation', 30, 'change_m3uvodcategoryrelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (119, 'Can delete M3U VOD Category Relation', 30, 'delete_m3uvodcategoryrelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (120, 'Can view M3U VOD Category Relation', 30, 'view_m3uvodcategoryrelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (121, 'Can add M3U Series Relation', 29, 'add_m3useriesrelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (122, 'Can change M3U Series Relation', 29, 'change_m3useriesrelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (123, 'Can delete M3U Series Relation', 29, 'delete_m3useriesrelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (124, 'Can view M3U Series Relation', 29, 'view_m3useriesrelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (125, 'Can add M3U Movie Relation', 28, 'add_m3umovierelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (126, 'Can change M3U Movie Relation', 28, 'change_m3umovierelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (127, 'Can delete M3U Movie Relation', 28, 'delete_m3umovierelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (128, 'Can view M3U Movie Relation', 28, 'view_m3umovierelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (129, 'Can add M3U Episode Relation', 27, 'add_m3uepisoderelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (130, 'Can change M3U Episode Relation', 27, 'change_m3uepisoderelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (131, 'Can delete M3U Episode Relation', 27, 'delete_m3uepisoderelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (132, 'Can view M3U Episode Relation', 27, 'view_m3uepisoderelation');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (133, 'Can add VOD Logo', 34, 'add_vodlogo');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (134, 'Can change VOD Logo', 34, 'change_vodlogo');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (135, 'Can delete VOD Logo', 34, 'delete_vodlogo');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (136, 'Can view VOD Logo', 34, 'view_vodlogo');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (137, 'Can add event subscription', 36, 'add_eventsubscription');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (138, 'Can change event subscription', 36, 'change_eventsubscription');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (139, 'Can delete event subscription', 36, 'delete_eventsubscription');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (140, 'Can view event subscription', 36, 'view_eventsubscription');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (141, 'Can add integration', 37, 'add_integration');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (142, 'Can change integration', 37, 'change_integration');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (143, 'Can delete integration', 37, 'delete_integration');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (144, 'Can view integration', 37, 'view_integration');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (145, 'Can add delivery log', 35, 'add_deliverylog');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (146, 'Can change delivery log', 35, 'change_deliverylog');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (147, 'Can delete delivery log', 35, 'delete_deliverylog');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (148, 'Can view delivery log', 35, 'view_deliverylog');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (149, 'Can add core settings', 38, 'add_coresettings');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (150, 'Can change core settings', 38, 'change_coresettings');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (151, 'Can delete core settings', 38, 'delete_coresettings');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (152, 'Can view core settings', 38, 'view_coresettings');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (153, 'Can add stream profile', 41, 'add_streamprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (154, 'Can change stream profile', 41, 'change_streamprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (155, 'Can delete stream profile', 41, 'delete_streamprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (156, 'Can view stream profile', 41, 'view_streamprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (157, 'Can add user agent', 44, 'add_useragent');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (158, 'Can change user agent', 44, 'change_useragent');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (159, 'Can delete user agent', 44, 'delete_useragent');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (160, 'Can view user agent', 44, 'view_useragent');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (161, 'Can add system event', 42, 'add_systemevent');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (162, 'Can change system event', 42, 'change_systemevent');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (163, 'Can delete system event', 42, 'delete_systemevent');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (164, 'Can view system event', 42, 'view_systemevent');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (165, 'Can add system notification', 43, 'add_systemnotification');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (166, 'Can change system notification', 43, 'change_systemnotification');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (167, 'Can delete system notification', 43, 'delete_systemnotification');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (168, 'Can view system notification', 43, 'view_systemnotification');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (169, 'Can add notification dismissal', 39, 'add_notificationdismissal');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (170, 'Can change notification dismissal', 39, 'change_notificationdismissal');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (171, 'Can delete notification dismissal', 39, 'delete_notificationdismissal');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (172, 'Can view notification dismissal', 39, 'view_notificationdismissal');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (173, 'Can add output profile', 40, 'add_outputprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (174, 'Can change output profile', 40, 'change_outputprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (175, 'Can delete output profile', 40, 'delete_outputprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (176, 'Can view output profile', 40, 'view_outputprofile');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (177, 'Can add log entry', 45, 'add_logentry');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (178, 'Can change log entry', 45, 'change_logentry');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (179, 'Can delete log entry', 45, 'delete_logentry');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (180, 'Can view log entry', 45, 'view_logentry');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (181, 'Can add permission', 47, 'add_permission');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (182, 'Can change permission', 47, 'change_permission');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (183, 'Can delete permission', 47, 'delete_permission');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (184, 'Can view permission', 47, 'view_permission');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (185, 'Can add group', 46, 'add_group');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (186, 'Can change group', 46, 'change_group');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (187, 'Can delete group', 46, 'delete_group');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (188, 'Can view group', 46, 'view_group');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (189, 'Can add content type', 48, 'add_contenttype');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (190, 'Can change content type', 48, 'change_contenttype');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (191, 'Can delete content type', 48, 'delete_contenttype');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (192, 'Can view content type', 48, 'view_contenttype');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (193, 'Can add session', 49, 'add_session');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (194, 'Can change session', 49, 'change_session');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (195, 'Can delete session', 49, 'delete_session');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (196, 'Can view session', 49, 'view_session');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (197, 'Can add crontab', 51, 'add_crontabschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (198, 'Can change crontab', 51, 'change_crontabschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (199, 'Can delete crontab', 51, 'delete_crontabschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (200, 'Can view crontab', 51, 'view_crontabschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (201, 'Can add interval', 52, 'add_intervalschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (202, 'Can change interval', 52, 'change_intervalschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (203, 'Can delete interval', 52, 'delete_intervalschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (204, 'Can view interval', 52, 'view_intervalschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (205, 'Can add periodic task', 53, 'add_periodictask');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (206, 'Can change periodic task', 53, 'change_periodictask');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (207, 'Can delete periodic task', 53, 'delete_periodictask');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (208, 'Can view periodic task', 53, 'view_periodictask');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (209, 'Can add periodic task track', 54, 'add_periodictasks');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (210, 'Can change periodic task track', 54, 'change_periodictasks');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (211, 'Can delete periodic task track', 54, 'delete_periodictasks');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (212, 'Can view periodic task track', 54, 'view_periodictasks');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (213, 'Can add solar event', 55, 'add_solarschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (214, 'Can change solar event', 55, 'change_solarschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (215, 'Can delete solar event', 55, 'delete_solarschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (216, 'Can view solar event', 55, 'view_solarschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (217, 'Can add clocked', 50, 'add_clockedschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (218, 'Can change clocked', 50, 'change_clockedschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (219, 'Can delete clocked', 50, 'delete_clockedschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (220, 'Can view clocked', 50, 'view_clockedschedule');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (221, 'Can add plugin config', 56, 'add_pluginconfig');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (222, 'Can change plugin config', 56, 'change_pluginconfig');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (223, 'Can delete plugin config', 56, 'delete_pluginconfig');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (224, 'Can view plugin config', 56, 'view_pluginconfig');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (225, 'Can add plugin repo', 57, 'add_pluginrepo');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (226, 'Can change plugin repo', 57, 'change_pluginrepo');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (227, 'Can delete plugin repo', 57, 'delete_pluginrepo');
INSERT INTO public.auth_permission (id, name, content_type_id, codename) VALUES (228, 'Can view plugin repo', 57, 'view_pluginrepo');


--
-- Data for Name: accounts_user_user_permissions; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: accounts_userappdata; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: auth_group_permissions; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: core_coresettings; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.core_coresettings (id, key, name, value) VALUES (14, 'dvr_settings', 'DVR Settings', '{"tv_template": "TV_Shows/{show}/S{season:02d}E{episode:02d}.mkv", "series_rules": [], "movie_template": "Movies/{title} ({year}).mkv", "comskip_enabled": false, "tv_fallback_dir": "TV_Shows", "pre_offset_minutes": 0, "comskip_custom_path": "", "post_offset_minutes": 0, "tv_fallback_template": "TV_Shows/{show}/{start}.mkv", "movie_fallback_template": "Movies/{start}.mkv"}');
INSERT INTO public.core_coresettings (id, key, name, value) VALUES (15, 'backup_settings', 'Backup Settings', '{"schedule_time": "03:00", "retention_count": 3, "schedule_enabled": true, "schedule_frequency": "daily", "schedule_day_of_week": 0, "schedule_cron_expression": ""}');
INSERT INTO public.core_coresettings (id, key, name, value) VALUES (6, 'network_access', 'Network Access', '{}');
INSERT INTO public.core_coresettings (id, key, name, value) VALUES (17, 'user_limit_settings', 'User Limit Settings', '{}');
INSERT INTO public.core_coresettings (id, key, name, value) VALUES (13, 'stream_settings', 'Stream Settings', '{"m3u_hash_key": "url", "default_user_agent": 1, "default_stream_profile": 1}');
INSERT INTO public.core_coresettings (id, key, name, value) VALUES (16, 'system_settings', 'System Settings', '{"time_zone": "UTC", "preferred_region": "us", "max_system_events": 100, "auto_import_mapped_files": true}');
INSERT INTO public.core_coresettings (id, key, name, value) VALUES (7, 'proxy_settings', 'Proxy Settings', '{"buffering_speed": 1.0, "redis_chunk_ttl": 60, "buffering_timeout": 15, "channel_shutdown_delay": 0, "channel_init_grace_period": 60, "channel_client_wait_period": 5}');
INSERT INTO public.core_coresettings (id, key, name, value) VALUES (18, 'reverse_proxy_auth', 'Reverse Proxy Auth', '{"header": "", "enabled": false}');


--
-- Data for Name: core_systemnotification; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.core_systemnotification (id, notification_key, notification_type, priority, source, title, message, action_data, is_active, admin_only, expires_at, created_at, updated_at) VALUES (1, 'network_security_m3u_epg', 'warning', 'high', 'developer', 'Network Access Security Warning', 'Your EPG/M3U output is accessible from any network. Consider restricting access to improve security.', '{"condition": ["m3u_epg_network_insecure"], "action_url": "/settings#network-access", "user_level": "admin", "action_text": "Review Settings", "max_version": null, "min_version": null}', true, true, NULL, '2026-11-09 19:03:40.129484+00', '2026-11-09 19:03:40.129491+00');


--
-- Data for Name: core_notificationdismissal; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: core_outputprofile; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.core_outputprofile (id, name, command, parameters, locked, is_active) VALUES (1, 'Media Server (AC3 Audio)', 'ffmpeg', '-fflags +discardcorrupt+genpts+nobuffer -probesize 512K -analyzeduration 0 -i pipe:0 -map 0 -max_interleave_delta 100K -c:v copy -c:a ac3 -b:a 384k -c:s copy -max_muxing_queue_size 4096 -flush_packets 1 -mpegts_flags +pat_pmt_at_frames+resend_headers+initial_discontinuity -f mpegts pipe:1', true, true);
INSERT INTO public.core_outputprofile (id, name, command, parameters, locked, is_active) VALUES (2, 'Web Player (AAC Audio)', 'ffmpeg', '-fflags +discardcorrupt+genpts+nobuffer -probesize 512K -analyzeduration 0 -i pipe:0 -map 0 -max_interleave_delta 100K -c:v copy -c:a aac -b:a 192k -ac 2 -c:s copy -max_muxing_queue_size 4096 -flush_packets 1 -mpegts_flags +pat_pmt_at_frames+resend_headers+initial_discontinuity -f mpegts pipe:1', true, true);


--
-- Data for Name: core_useragent; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.core_useragent (id, name, user_agent, description, is_active, created_at, updated_at) VALUES (2, 'VLC', 'VLC/3.0.21 LibVLC/3.0.21', '', true, '2026-11-09 19:03:36.797367+00', '2026-11-09 19:03:36.797372+00');
INSERT INTO public.core_useragent (id, name, user_agent, description, is_active, created_at, updated_at) VALUES (3, 'Chrome', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.3', '', true, '2026-11-09 19:03:36.79764+00', '2026-11-09 19:03:36.797648+00');
INSERT INTO public.core_useragent (id, name, user_agent, description, is_active, created_at, updated_at) VALUES (1, 'TiviMate', 'TiviMate/5.1.6 (Android 12)', '', true, '2026-11-09 19:03:36.796929+00', '2026-11-09 19:03:37.217022+00');


--
-- Data for Name: core_streamprofile; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.core_streamprofile (id, name, command, parameters, is_active, user_agent_id, locked) VALUES (3, 'Proxy', '', '', true, NULL, true);
INSERT INTO public.core_streamprofile (id, name, command, parameters, is_active, user_agent_id, locked) VALUES (4, 'Redirect', '', '', true, NULL, true);
INSERT INTO public.core_streamprofile (id, name, command, parameters, is_active, user_agent_id, locked) VALUES (5, 'VLC', 'cvlc', '-vv -I dummy --no-video-title-show --play-and-exit --http-user-agent {userAgent} {streamUrl} --sout #standard{access=file,mux=ts,dst=-}', true, 1, true);
INSERT INTO public.core_streamprofile (id, name, command, parameters, is_active, user_agent_id, locked) VALUES (2, 'Streamlink', 'streamlink', '--http-header User-Agent={userAgent} --ffmpeg-fout mpegts --stdout {streamUrl} best', true, 1, true);
INSERT INTO public.core_streamprofile (id, name, command, parameters, is_active, user_agent_id, locked) VALUES (1, 'FFmpeg', 'ffmpeg', '-fflags +genpts+discardcorrupt -user_agent {userAgent} -i {streamUrl} -map 0 -max_interleave_delta 100K -c copy -mpegts_flags +pat_pmt_at_frames+resend_headers+initial_discontinuity -f mpegts pipe:1', true, 1, true);


--
-- Data for Name: core_systemevent; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: dashboard_settings; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: dispatcharr_channels_channelgroup; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: dispatcharr_channels_logo; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: django_celery_beat_clockedschedule; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: django_celery_beat_crontabschedule; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.django_celery_beat_crontabschedule (id, minute, hour, day_of_week, day_of_month, month_of_year, timezone) VALUES (1, '00', '03', '*', '*', '*', 'UTC');


--
-- Data for Name: django_celery_beat_intervalschedule; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.django_celery_beat_intervalschedule (id, every, period) VALUES (1, 24, 'hours');
INSERT INTO public.django_celery_beat_intervalschedule (id, every, period) VALUES (2, 6, 'hours');


--
-- Data for Name: django_celery_beat_solarschedule; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: django_celery_beat_periodictask; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.django_celery_beat_periodictask (id, name, task, args, kwargs, queue, exchange, routing_key, expires, enabled, last_run_at, total_run_count, date_changed, description, crontab_id, interval_id, solar_id, one_off, start_time, priority, headers, clocked_id, expire_seconds) VALUES (1, 'm3u_account-refresh-1', 'apps.m3u.tasks.refresh_single_m3u_account', '[]', '{"account_id": 1}', NULL, NULL, NULL, NULL, true, NULL, 0, '2026-11-09 19:03:37.780005+00', '', NULL, 1, NULL, false, NULL, NULL, '{}', NULL, NULL);
INSERT INTO public.django_celery_beat_periodictask (id, name, task, args, kwargs, queue, exchange, routing_key, expires, enabled, last_run_at, total_run_count, date_changed, description, crontab_id, interval_id, solar_id, one_off, start_time, priority, headers, clocked_id, expire_seconds) VALUES (2, 'backup-scheduled-task', 'apps.backups.tasks.scheduled_backup_task', '[]', '{"retention_count": 3}', NULL, NULL, NULL, NULL, true, NULL, 0, '2026-11-09 19:03:40.034543+00', '', 1, NULL, NULL, false, NULL, NULL, '{}', NULL, NULL);
INSERT INTO public.django_celery_beat_periodictask (id, name, task, args, kwargs, queue, exchange, routing_key, expires, enabled, last_run_at, total_run_count, date_changed, description, crontab_id, interval_id, solar_id, one_off, start_time, priority, headers, clocked_id, expire_seconds) VALUES (3, 'plugin-repo-refresh-task', 'apps.plugins.tasks.refresh_plugin_repos', '[]', '{}', NULL, NULL, NULL, NULL, true, NULL, 0, '2026-11-09 19:03:40.165634+00', '', NULL, 2, NULL, false, NULL, NULL, '{}', NULL, NULL);


--
-- Data for Name: epg_epgsource; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: epg_epgdata; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: m3u_servergroup; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: m3u_m3uaccount; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.m3u_m3uaccount (id, name, server_url, max_streams, is_active, created_at, updated_at, user_agent_id, server_group_id, locked, stream_profile_id, custom_properties, refresh_interval, refresh_task_id, file_path, stale_stream_days, account_type, password, username, last_message, status, priority) VALUES (1, 'custom', NULL, 0, true, '2026-11-09 19:03:36.813312+00', '2026-11-09 19:03:36.813318+00', NULL, NULL, true, NULL, NULL, 24, 1, NULL, 7, 'STD', NULL, NULL, NULL, 'idle', 0);


--
-- Data for Name: dispatcharr_channels_channel; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: dispatcharr_channels_channelgroupm3uaccount; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: dispatcharr_channels_channeloverride; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: dispatcharr_channels_channelprofilemembership; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: dispatcharr_channels_stream; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: dispatcharr_channels_channelstream; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: dispatcharr_channels_recording; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: dispatcharr_channels_recurringrecordingrule; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: dispatcharr_connect_integration; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: dispatcharr_connect_eventsubscription; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: dispatcharr_connect_deliverylog; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: django_admin_log; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: django_celery_beat_periodictasks; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.django_celery_beat_periodictasks (ident, last_update) VALUES (1, '2026-11-09 19:03:40.165861+00');


--
-- Data for Name: django_migrations; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.django_migrations (id, app, name, applied) VALUES (1, 'epg', '0001_initial', '2026-11-09 19:03:36.762877+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (2, 'epg', '0002_epgsource_file_path', '2026-11-09 19:03:36.764012+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (3, 'epg', '0003_alter_epgdata_tvg_id', '2026-11-09 19:03:36.765729+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (4, 'epg', '0004_epgdata_epg_source_alter_epgdata_tvg_id', '2026-11-09 19:03:36.772313+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (5, 'core', '0001_initial', '2026-11-09 19:03:36.777559+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (6, 'm3u', '0001_initial', '2026-11-09 19:03:36.789826+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (7, 'm3u', '0002_m3uaccount_locked', '2026-11-09 19:03:36.792623+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (8, 'core', '0002_preload_user_agents', '2026-11-09 19:03:36.797829+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (9, 'core', '0003_preload_stream_profiles', '2026-11-09 19:03:36.802605+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (10, 'core', '0004_preload_core_settings', '2026-11-09 19:03:36.806409+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (11, 'm3u', '0003_create_custom_account', '2026-11-09 19:03:36.814309+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (12, 'dispatcharr_channels', '0001_initial', '2026-11-09 19:03:36.835298+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (13, 'dispatcharr_channels', '0002_rename_channel_name_channel_name_and_more', '2026-11-09 19:03:36.847169+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (14, 'dispatcharr_channels', '0003_channel_uuid', '2026-11-09 19:03:36.857135+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (15, 'dispatcharr_channels', '0004_stream_is_custom', '2026-11-09 19:03:36.861062+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (16, 'dispatcharr_channels', '0005_stream_channel_group_stream_last_seen_and_more', '2026-11-09 19:03:36.87757+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (17, 'dispatcharr_channels', '0006_migrate_stream_groups', '2026-11-09 19:03:36.883246+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (18, 'dispatcharr_channels', '0007_remove_stream_group_name', '2026-11-09 19:03:36.887449+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (19, 'dispatcharr_channels', '0008_stream_stream_hash', '2026-11-09 19:03:36.895921+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (20, 'dispatcharr_channels', '0009_remove_channel_tvg_name_channel_epg_data', '2026-11-09 19:03:36.905259+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (21, 'dispatcharr_channels', '0010_stream_custom_properties', '2026-11-09 19:03:36.909816+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (22, 'dispatcharr_channels', '0011_logo_remove_channel_logo_file_and_more', '2026-11-09 19:03:36.926247+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (23, 'dispatcharr_channels', '0012_channelprofile_channelprofilemembership', '2026-11-09 19:03:36.934794+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (24, 'dispatcharr_channels', '0013_alter_logo_url', '2026-11-09 19:03:36.937247+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (25, 'dispatcharr_channels', '0014_recording', '2026-11-09 19:03:36.944165+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (26, 'dispatcharr_channels', '0015_recording_custom_properties', '2026-11-09 19:03:36.947838+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (27, 'dispatcharr_channels', '0016_channelstream_unique_channel_stream', '2026-11-09 19:03:36.959569+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (28, 'dispatcharr_channels', '0017_alter_channelgroup_name', '2026-11-09 19:03:36.962762+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (29, 'dispatcharr_channels', '0018_channelgroupm3uaccount_custom_properties_and_more', '2026-11-09 19:03:36.966467+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (30, 'dispatcharr_channels', '0019_channel_tvc_guide_stationid', '2026-11-09 19:03:36.971244+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (31, 'dispatcharr_channels', '0020_alter_channel_channel_number', '2026-11-09 19:03:36.977397+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (32, 'dispatcharr_channels', '0021_channel_user_level', '2026-11-09 19:03:36.982886+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (33, 'contenttypes', '0001_initial', '2026-11-09 19:03:36.98485+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (34, 'contenttypes', '0002_remove_content_type_name', '2026-11-09 19:03:36.9926+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (35, 'auth', '0001_initial', '2026-11-09 19:03:37.000868+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (36, 'auth', '0002_alter_permission_name_max_length', '2026-11-09 19:03:37.002678+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (37, 'auth', '0003_alter_user_email_max_length', '2026-11-09 19:03:37.004698+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (38, 'auth', '0004_alter_user_username_opts', '2026-11-09 19:03:37.006957+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (39, 'auth', '0005_alter_user_last_login_null', '2026-11-09 19:03:37.008979+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (40, 'auth', '0006_require_contenttypes_0002', '2026-11-09 19:03:37.009258+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (41, 'auth', '0007_alter_validators_add_error_messages', '2026-11-09 19:03:37.011273+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (42, 'auth', '0008_alter_user_username_max_length', '2026-11-09 19:03:37.015784+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (43, 'auth', '0009_alter_user_last_name_max_length', '2026-11-09 19:03:37.017798+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (44, 'auth', '0010_alter_group_name_max_length', '2026-11-09 19:03:37.019959+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (45, 'auth', '0011_update_proxy_permissions', '2026-11-09 19:03:37.02707+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (46, 'auth', '0012_alter_user_first_name_max_length', '2026-11-09 19:03:37.029258+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (47, 'accounts', '0001_initial', '2026-11-09 19:03:37.043827+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (48, 'accounts', '0002_remove_user_channel_groups_user_channel_profiles_and_more', '2026-11-09 19:03:37.077236+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (49, 'accounts', '0003_alter_user_custom_properties', '2026-11-09 19:03:37.081568+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (50, 'accounts', '0004_user_api_key', '2026-11-09 19:03:37.085643+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (51, 'accounts', '0005_alter_user_managers', '2026-11-09 19:03:37.089017+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (52, 'accounts', '0006_user_stream_limit', '2026-11-09 19:03:37.095362+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (53, 'accounts', '0007_user_app_data', '2026-11-09 19:03:37.098921+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (54, 'accounts', '0008_userappdata_and_remove_user_app_data', '2026-11-09 19:03:37.124434+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (55, 'admin', '0001_initial', '2026-11-09 19:03:37.13527+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (56, 'admin', '0002_logentry_remove_auto_add', '2026-11-09 19:03:37.139106+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (57, 'admin', '0003_logentry_add_action_flag_choices', '2026-11-09 19:03:37.142707+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (58, 'core', '0005_streamprofile_locked_alter_streamprofile_command_and_more', '2026-11-09 19:03:37.159372+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (59, 'core', '0006_set_locked_stream_profiles', '2026-11-09 19:03:37.173282+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (60, 'core', '0007_create_proxy_and_redirect_stream_profiles', '2026-11-09 19:03:37.183063+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (61, 'core', '0008_rename_profile_name_streamprofile_name_and_more', '2026-11-09 19:03:37.187669+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (62, 'core', '0009_m3u_hash_settings', '2026-11-09 19:03:37.197281+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (63, 'core', '0010_reload_additional_settings', '2026-11-09 19:03:37.206643+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (64, 'core', '0011_fix_stream_profiles_and_user_agents', '2026-11-09 19:03:37.217328+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (65, 'core', '0012_default_active_m3u_accounts', '2026-11-09 19:03:37.227349+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (66, 'core', '0013_default_network_access_settings', '2026-11-09 19:03:37.237237+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (67, 'core', '0014_default_proxy_settings', '2026-11-09 19:03:37.249707+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (68, 'core', '0015_dvr_templates', '2026-11-09 19:03:37.261535+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (69, 'core', '0016_update_dvr_template_paths', '2026-11-09 19:03:37.272642+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (70, 'core', '0017_systemevent', '2026-11-09 19:03:37.275312+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (71, 'core', '0018_alter_systemevent_event_type', '2026-11-09 19:03:37.276307+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (72, 'core', '0019_add_vlc_stream_profile', '2026-11-09 19:03:37.286894+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (73, 'core', '0020_change_coresettings_value_to_jsonfield', '2026-11-09 19:03:37.318924+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (74, 'core', '0021_systemnotification_notificationdismissal', '2026-11-09 19:03:37.335735+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (75, 'core', '022_default_user_limit_settings', '2026-11-09 19:03:37.350183+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (76, 'core', '0023_alter_systemevent_event_type', '2026-11-09 19:03:37.351243+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (77, 'core', '0024_outputprofile', '2026-11-09 19:03:37.364956+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (78, 'core', '0025_move_preferred_region_and_auto_import_to_system_settings', '2026-11-09 19:03:37.377899+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (79, 'core', '0026_add_channel_client_wait_period', '2026-11-09 19:03:37.389501+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (80, 'core', '0027_vlc_play_and_exit', '2026-11-09 19:03:37.400369+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (81, 'core', '0028_alter_systemevent_event_type', '2026-11-09 19:03:37.401452+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (82, 'core', '0029_update_default_stream_and_output_profiles', '2026-11-09 19:03:37.413339+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (83, 'core', '0030_rename_default_stream_profile_names', '2026-11-09 19:03:37.429634+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (84, 'core', '0031_default_reverse_proxy_auth_settings', '2026-11-09 19:03:37.440875+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (85, 'core', '0032_limit_default_interleave_delta', '2026-11-09 19:03:37.452056+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (86, 'dashboard', '0001_initial', '2026-11-09 19:03:37.454627+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (87, 'm3u', '0004_m3uaccount_stream_profile', '2026-11-09 19:03:37.465333+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (88, 'django_celery_beat', '0001_initial', '2026-11-09 19:03:37.472147+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (89, 'django_celery_beat', '0002_auto_20161118_0346', '2026-11-09 19:03:37.475809+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (90, 'django_celery_beat', '0003_auto_20161209_0049', '2026-11-09 19:03:37.478728+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (91, 'django_celery_beat', '0004_auto_20170221_0000', '2026-11-09 19:03:37.480267+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (179, 'vod', '0001_initial', '2026-11-09 19:03:39.099604+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (92, 'django_celery_beat', '0005_add_solarschedule_events_choices', '2026-11-09 19:03:37.481797+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (93, 'django_celery_beat', '0006_auto_20180322_0932', '2026-11-09 19:03:37.496716+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (94, 'django_celery_beat', '0007_auto_20180521_0826', '2026-11-09 19:03:37.504306+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (95, 'django_celery_beat', '0008_auto_20180914_1922', '2026-11-09 19:03:37.575911+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (96, 'django_celery_beat', '0006_auto_20180210_1226', '2026-11-09 19:03:37.586173+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (97, 'django_celery_beat', '0006_periodictask_priority', '2026-11-09 19:03:37.59027+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (98, 'django_celery_beat', '0009_periodictask_headers', '2026-11-09 19:03:37.594313+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (99, 'django_celery_beat', '0010_auto_20190429_0326', '2026-11-09 19:03:37.701792+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (100, 'django_celery_beat', '0011_auto_20190508_0153', '2026-11-09 19:03:37.708928+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (101, 'django_celery_beat', '0012_periodictask_expire_seconds', '2026-11-09 19:03:37.713611+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (102, 'django_celery_beat', '0013_auto_20200609_0727', '2026-11-09 19:03:37.718298+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (103, 'django_celery_beat', '0014_remove_clockedschedule_enabled', '2026-11-09 19:03:37.720494+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (104, 'django_celery_beat', '0015_edit_solarschedule_events_choices', '2026-11-09 19:03:37.722628+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (105, 'django_celery_beat', '0016_alter_crontabschedule_timezone', '2026-11-09 19:03:37.727668+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (106, 'django_celery_beat', '0017_alter_crontabschedule_month_of_year', '2026-11-09 19:03:37.73188+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (107, 'django_celery_beat', '0018_improve_crontab_helptext', '2026-11-09 19:03:37.735712+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (108, 'django_celery_beat', '0019_alter_periodictasks_options', '2026-11-09 19:03:37.736808+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (109, 'm3u', '0005_m3uaccount_custom_properties_and_more', '2026-11-09 19:03:37.75983+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (110, 'm3u', '0006_populate_periodic_tasks', '2026-11-09 19:03:37.781+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (111, 'm3u', '0007_remove_m3uaccount_uploaded_file_m3uaccount_file_path', '2026-11-09 19:03:37.795979+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (112, 'm3u', '0008_m3uaccount_stale_stream_days', '2026-11-09 19:03:37.804066+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (113, 'm3u', '0009_m3uaccount_account_type_m3uaccount_password_and_more', '2026-11-09 19:03:37.827567+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (114, 'm3u', '0010_add_status_fields_and_remove_auto_now', '2026-11-09 19:03:37.851124+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (115, 'm3u', '0011_alter_m3uaccount_status', '2026-11-09 19:03:37.862083+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (116, 'm3u', '0012_alter_m3uaccount_refresh_interval', '2026-11-09 19:03:37.869576+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (117, 'm3u', '0013_alter_m3ufilter_filter_type', '2026-11-09 19:03:37.874455+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (118, 'm3u', '0014_alter_m3ufilter_options_m3ufilter_order', '2026-11-09 19:03:37.883144+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (119, 'm3u', '0015_alter_m3ufilter_options_m3ufilter_custom_properties', '2026-11-09 19:03:37.891792+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (120, 'm3u', '0016_m3uaccount_priority', '2026-11-09 19:03:37.899672+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (121, 'm3u', '0017_alter_m3uaccount_custom_properties_and_more', '2026-11-09 19:03:37.921499+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (122, 'm3u', '0018_add_profile_custom_properties', '2026-11-09 19:03:37.927396+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (123, 'm3u', '0019_m3uaccountprofile_exp_date', '2026-11-09 19:03:37.951838+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (124, 'epg', '0005_programdata_custom_properties_and_more', '2026-11-09 19:03:37.957011+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (125, 'epg', '0006_epgsource_refresh_interval_epgsource_refresh_task', '2026-11-09 19:03:37.973297+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (126, 'epg', '0007_populate_periodic_tasks', '2026-11-09 19:03:37.988827+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (127, 'epg', '0008_epgsource_created_at_epgsource_updated_at', '2026-11-09 19:03:38.001199+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (128, 'epg', '0009_alter_epgsource_created_at_and_more', '2026-11-09 19:03:38.011799+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (129, 'epg', '0007_epgsource_status_epgsource_last_error', '2026-11-09 19:03:38.024184+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (130, 'epg', '0010_merge_20250503_2147', '2026-11-09 19:03:38.024531+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (131, 'epg', '0011_update_epgsource_fields', '2026-11-09 19:03:38.060308+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (132, 'epg', '0012_alter_epgsource_status', '2026-11-09 19:03:38.065761+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (133, 'epg', '0013_alter_epgsource_refresh_interval', '2026-11-09 19:03:38.071233+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (134, 'epg', '0014_epgsource_extracted_file_path', '2026-11-09 19:03:38.076835+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (135, 'epg', '0015_alter_programdata_custom_properties', '2026-11-09 19:03:38.080235+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (136, 'epg', '0016_epgdata_icon_url', '2026-11-09 19:03:38.08376+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (137, 'epg', '0017_alter_epgsource_url', '2026-11-09 19:03:38.089461+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (138, 'epg', '0018_epgsource_custom_properties_and_more', '2026-11-09 19:03:38.100414+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (139, 'epg', '0019_alter_programdata_sub_title', '2026-11-09 19:03:38.103332+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (140, 'epg', '0020_migrate_time_to_starttime_placeholders', '2026-11-09 19:03:38.121596+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (141, 'epg', '0021_epgsource_priority', '2026-11-09 19:03:38.127605+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (142, 'epg', '0022_alter_epgdata_name', '2026-11-09 19:03:38.130956+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (143, 'dispatcharr_channels', '0022_channel_auto_created_channel_auto_created_by_and_more', '2026-11-09 19:03:38.161787+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (144, 'dispatcharr_channels', '0023_stream_stream_stats_stream_stream_stats_updated_at', '2026-11-09 19:03:38.173263+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (145, 'dispatcharr_channels', '0024_alter_channelgroupm3uaccount_channel_group', '2026-11-09 19:03:38.187657+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (146, 'dispatcharr_channels', '0025_alter_channelgroupm3uaccount_custom_properties_and_more', '2026-11-09 19:03:38.217091+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (147, 'dispatcharr_channels', '0026_recurringrecordingrule', '2026-11-09 19:03:38.233399+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (148, 'dispatcharr_channels', '0027_recurringrecordingrule_end_date_and_more', '2026-11-09 19:03:38.243169+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (149, 'dispatcharr_channels', '0028_channel_created_at_channel_updated_at', '2026-11-09 19:03:38.259957+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (150, 'dispatcharr_channels', '0029_backfill_custom_stream_hashes', '2026-11-09 19:03:38.279232+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (151, 'dispatcharr_channels', '0030_alter_stream_url', '2026-11-09 19:03:38.293942+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (152, 'dispatcharr_channels', '0031_channelgroupm3uaccount_is_stale_and_more', '2026-11-09 19:03:38.311485+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (153, 'dispatcharr_channels', '0032_channel_is_adult_stream_is_adult', '2026-11-09 19:03:38.326525+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (154, 'dispatcharr_channels', '0033_stream_id_stream_chno', '2026-11-09 19:03:38.370823+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (155, 'dispatcharr_channels', '0034_remove_stream_dispatcharr_stream_id_idx_and_more', '2026-11-09 19:03:38.412385+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (156, 'dispatcharr_channels', '0035_alter_channel_name_alter_stream_name', '2026-11-09 19:03:38.440148+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (157, 'dispatcharr_channels', '0036_alter_stream_name', '2026-11-09 19:03:38.455471+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (158, 'dispatcharr_channels', '0037_auto_sync_overhaul', '2026-11-09 19:03:38.601699+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (159, 'dispatcharr_channels', '0038_add_catchup_fields', '2026-11-09 19:03:38.667985+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (160, 'dispatcharr_channels', '0039_add_radio_fields', '2026-11-09 19:03:38.690581+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (161, 'dispatcharr_channels', '0040_channelgroup_custom_relation', '2026-11-09 19:03:38.709008+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (162, 'dispatcharr_connect', '0001_initial', '2026-11-09 19:03:38.716605+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (163, 'dispatcharr_connect', '0002_alter_eventsubscription_event', '2026-11-09 19:03:38.718379+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (164, 'dispatcharr_connect', '0003_alter_eventsubscription_event', '2026-11-09 19:03:38.720166+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (165, 'dispatcharr_connect', '0004_alter_eventsubscription_event', '2026-11-09 19:03:38.722218+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (166, 'dispatcharr_connect', '0005_alter_eventsubscription_event', '2026-11-09 19:03:38.723998+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (167, 'epg', '0023_epgsource_programme_index', '2026-11-09 19:03:38.729771+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (168, 'epg', '0024_remove_epgsource_api_key_epgsource_password_and_more', '2026-11-09 19:03:38.793937+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (169, 'epg', '0025_programdata_epg_id_index', '2026-11-09 19:03:38.797748+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (170, 'epg', '0026_epgsourceindex', '2026-11-09 19:03:38.842551+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (171, 'epg', '0027_programdata_epg_start_end_index', '2026-11-09 19:03:38.846364+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (172, 'epg', '0028_programdata_epg_end_index', '2026-11-09 19:03:38.849904+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (173, 'hdhr', '0001_initial', '2026-11-09 19:03:38.851767+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (174, 'plugins', '0001_initial', '2026-11-09 19:03:38.853839+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (175, 'plugins', '0002_pluginrepo', '2026-11-09 19:03:38.88322+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (176, 'plugins', '0003_update_official_repo_url', '2026-11-09 19:03:38.905318+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (177, 'plugins', '0004_plugin_schedule_ownership', '2026-11-09 19:03:38.909298+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (178, 'sessions', '0001_initial', '2026-11-09 19:03:38.911181+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (180, 'vod', '0002_add_last_seen_with_default', '2026-11-09 19:03:39.125732+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (181, 'vod', '0003_vodlogo_alter_movie_logo_alter_series_logo', '2026-11-09 19:03:39.222443+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (182, 'vod', '0004_m3uepisoderelation_series_relation', '2026-11-09 19:03:39.245182+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (183, 'vod', '0005_movie_is_adult', '2026-11-09 19:03:39.271146+00');
INSERT INTO public.django_migrations (id, app, name, applied) VALUES (184, 'vod', '0006_movie_series_language', '2026-11-09 19:03:39.322553+00');


--
-- Data for Name: django_session; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: epg_epgsourceindex; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: epg_programdata; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: epg_sdprogrammd5; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: epg_sdschedulemd5; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: hdhr_hdhrdevice; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: m3u_m3uaccountprofile; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.m3u_m3uaccountprofile (id, name, is_default, max_streams, is_active, search_pattern, replace_pattern, current_viewers, m3u_account_id, custom_properties, exp_date) VALUES (1, 'custom Default', true, 0, true, '^(.*)$', '$1', 0, 1, '{}', NULL);


--
-- Data for Name: m3u_m3ufilter; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: plugins_pluginrepo; Type: TABLE DATA; Schema: public; Owner: -
--

INSERT INTO public.plugins_pluginrepo (id, name, url, is_official, enabled, cached_manifest, last_fetched, public_key, signature_verified, last_fetch_status, created_at, updated_at) VALUES (1, 'Dispatcharr Official', 'https://dispatcharr.github.io/Plugins/manifest.json', true, true, '{}', NULL, '', NULL, '', '2026-11-09 19:03:38.875488+00', '2026-11-09 19:03:38.875493+00');


--
-- Data for Name: plugins_pluginconfig; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: vod_series; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: vod_episode; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: vod_vodcategory; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: vod_m3useriesrelation; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: vod_m3uepisoderelation; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: vod_movie; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: vod_m3umovierelation; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: vod_m3uvodcategoryrelation; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Data for Name: vod_vodlogo; Type: TABLE DATA; Schema: public; Owner: -
--



--
-- Name: accounts_user_channel_profiles_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.accounts_user_channel_profiles_id_seq', 1, false);


--
-- Name: accounts_user_groups_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.accounts_user_groups_id_seq', 1, false);


--
-- Name: accounts_user_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.accounts_user_id_seq', 1, false);


--
-- Name: accounts_user_user_permissions_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.accounts_user_user_permissions_id_seq', 1, false);


--
-- Name: accounts_userappdata_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.accounts_userappdata_id_seq', 1, false);


--
-- Name: auth_group_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.auth_group_id_seq', 1, false);


--
-- Name: auth_group_permissions_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.auth_group_permissions_id_seq', 1, false);


--
-- Name: auth_permission_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.auth_permission_id_seq', 228, true);


--
-- Name: core_coresettings_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.core_coresettings_id_seq', 18, true);


--
-- Name: core_notificationdismissal_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.core_notificationdismissal_id_seq', 1, false);


--
-- Name: core_outputprofile_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.core_outputprofile_id_seq', 2, true);


--
-- Name: core_streamprofile_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.core_streamprofile_id_seq', 5, true);


--
-- Name: core_systemevent_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.core_systemevent_id_seq', 1, false);


--
-- Name: core_systemnotification_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.core_systemnotification_id_seq', 1, true);


--
-- Name: core_useragent_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.core_useragent_id_seq', 3, true);


--
-- Name: dashboard_settings_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.dashboard_settings_id_seq', 1, false);


--
-- Name: dispatcharr_channels_channel_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.dispatcharr_channels_channel_id_seq', 1, false);


--
-- Name: dispatcharr_channels_channelgroup_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.dispatcharr_channels_channelgroup_id_seq', 1, false);


--
-- Name: dispatcharr_channels_channelgroupm3uaccount_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.dispatcharr_channels_channelgroupm3uaccount_id_seq', 1, false);


--
-- Name: dispatcharr_channels_channeloverride_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.dispatcharr_channels_channeloverride_id_seq', 1, false);


--
-- Name: dispatcharr_channels_channelprofile_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.dispatcharr_channels_channelprofile_id_seq', 1, false);


--
-- Name: dispatcharr_channels_channelprofilemembership_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.dispatcharr_channels_channelprofilemembership_id_seq', 1, false);


--
-- Name: dispatcharr_channels_channelstream_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.dispatcharr_channels_channelstream_id_seq', 1, false);


--
-- Name: dispatcharr_channels_logo_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.dispatcharr_channels_logo_id_seq', 1, false);


--
-- Name: dispatcharr_channels_recording_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.dispatcharr_channels_recording_id_seq', 1, false);


--
-- Name: dispatcharr_channels_recurringrecordingrule_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.dispatcharr_channels_recurringrecordingrule_id_seq', 1, false);


--
-- Name: dispatcharr_channels_stream_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.dispatcharr_channels_stream_id_seq', 1, false);


--
-- Name: dispatcharr_connect_deliverylog_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.dispatcharr_connect_deliverylog_id_seq', 1, false);


--
-- Name: dispatcharr_connect_eventsubscription_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.dispatcharr_connect_eventsubscription_id_seq', 1, false);


--
-- Name: dispatcharr_connect_integration_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.dispatcharr_connect_integration_id_seq', 1, false);


--
-- Name: django_admin_log_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.django_admin_log_id_seq', 1, false);


--
-- Name: django_celery_beat_clockedschedule_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.django_celery_beat_clockedschedule_id_seq', 1, false);


--
-- Name: django_celery_beat_crontabschedule_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.django_celery_beat_crontabschedule_id_seq', 1, true);


--
-- Name: django_celery_beat_intervalschedule_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.django_celery_beat_intervalschedule_id_seq', 2, true);


--
-- Name: django_celery_beat_periodictask_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.django_celery_beat_periodictask_id_seq', 3, true);


--
-- Name: django_celery_beat_solarschedule_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.django_celery_beat_solarschedule_id_seq', 1, false);


--
-- Name: django_content_type_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.django_content_type_id_seq', 57, true);


--
-- Name: django_migrations_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.django_migrations_id_seq', 184, true);


--
-- Name: epg_epgdata_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.epg_epgdata_id_seq', 1, false);


--
-- Name: epg_epgsource_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.epg_epgsource_id_seq', 1, false);


--
-- Name: epg_programdata_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.epg_programdata_id_seq', 1, false);


--
-- Name: epg_sdprogrammd5_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.epg_sdprogrammd5_id_seq', 1, false);


--
-- Name: epg_sdschedulemd5_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.epg_sdschedulemd5_id_seq', 1, false);


--
-- Name: hdhr_hdhrdevice_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.hdhr_hdhrdevice_id_seq', 1, false);


--
-- Name: m3u_m3uaccount_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.m3u_m3uaccount_id_seq', 1, true);


--
-- Name: m3u_m3uaccountprofile_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.m3u_m3uaccountprofile_id_seq', 1, true);


--
-- Name: m3u_m3ufilter_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.m3u_m3ufilter_id_seq', 1, false);


--
-- Name: m3u_servergroup_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.m3u_servergroup_id_seq', 1, false);


--
-- Name: plugins_pluginconfig_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.plugins_pluginconfig_id_seq', 1, false);


--
-- Name: plugins_pluginrepo_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.plugins_pluginrepo_id_seq', 1, true);


--
-- Name: vod_episode_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.vod_episode_id_seq', 1, false);


--
-- Name: vod_m3uepisoderelation_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.vod_m3uepisoderelation_id_seq', 1, false);


--
-- Name: vod_m3umovierelation_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.vod_m3umovierelation_id_seq', 1, false);


--
-- Name: vod_m3useriesrelation_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.vod_m3useriesrelation_id_seq', 1, false);


--
-- Name: vod_m3uvodcategoryrelation_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.vod_m3uvodcategoryrelation_id_seq', 1, false);


--
-- Name: vod_movie_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.vod_movie_id_seq', 1, false);


--
-- Name: vod_series_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.vod_series_id_seq', 1, false);


--
-- Name: vod_vodcategory_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.vod_vodcategory_id_seq', 1, false);


--
-- Name: vod_vodlogo_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.vod_vodlogo_id_seq', 1, false);


--
-- PostgreSQL database dump complete
--


