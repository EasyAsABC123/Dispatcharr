def process_m3u_batch_direct(account_id, batch, groups, hash_keys, compiled_filters=None):
    """Processes a batch of M3U streams using bulk operations with thread-safe DB connections.

    ``compiled_filters`` should be pre-built once per account refresh and shared
    across batch workers. Pass an empty list when the account has no filters.
    """
    from django.db import connections

    # Ensure clean database connections for threading
    connections.close_all()

    account = M3UAccount.objects.select_related("user_agent").get(id=account_id)

    if compiled_filters is None:
        compiled_filters = _compile_m3u_stream_filters(account.filters.order_by("order"))

    streams_to_create = []
    streams_to_update = []
    streams_to_touch = []
    stream_hashes = {}

    name_max_length = Stream._meta.get_field('name').max_length

    logger.debug(f"Processing batch of {len(batch)} for M3U account {account_id}")
    if compiled_filters:
        logger.debug(
            "Using compiled filters: %s",
            [filter_obj.regex_pattern for _, filter_obj in compiled_filters],
        )
    for stream_info in batch:
        try:
            name, url = stream_info["name"], stream_info["url"]

            # Validate URL length - maximum of 4096 characters
            if url and len(url) > 4096:
                logger.warning(f"Skipping stream '{name}': URL too long ({len(url)} characters, max 4096)")
                continue

            name = truncate_with_warning(
                name, max_length=name_max_length, label="Stream name"
            )

            tvg_id, tvg_logo = get_case_insensitive_attr(
                stream_info["attributes"], "tvg-id", ""
            ), get_case_insensitive_attr(stream_info["attributes"], "tvg-logo", "")
            group_title = get_case_insensitive_attr(
                stream_info["attributes"], "group-title", "Default Group"
            )
            logger.trace("Processing stream: %s - %s in group %s", name, url, group_title)

            if compiled_filters and not _stream_passes_m3u_filters(
                name, url, group_title, compiled_filters,
            ):
                logger.debug("Stream excluded by filter, skipping.")
                continue

            # Filter out disabled groups for this account
            if group_title not in groups:
                logger.debug(
                    f"Skipping stream in disabled or excluded group: {group_title}"
                )
                continue

            # Determine provider-specific fields first
            provider_stream_id = None
            channel_num = None
            account_type_for_hash = None

            if account.account_type == M3UAccount.Types.XC:
                account_type_for_hash = 'XC'
                raw_stream_id = stream_info["attributes"].get("stream_id", "")
                if raw_stream_id:
                    try:
                        provider_stream_id = int(raw_stream_id)
                    except (ValueError, TypeError):
                        pass
                raw_num = stream_info["attributes"].get("num")
                if raw_num is not None:
                    try:
                        channel_num = float(raw_num)
                    except (ValueError, TypeError):
                        pass
            else:
                # For standard M3U accounts, check for tvg-chno or channel-number
                tvg_chno = get_case_insensitive_attr(stream_info["attributes"], "tvg-chno", None)
                if tvg_chno is None:
                    tvg_chno = get_case_insensitive_attr(stream_info["attributes"], "channel-number", None)
                if tvg_chno is not None:
                    try:
                        channel_num = float(tvg_chno)
                    except (ValueError, TypeError):
                        pass

            # Generate hash once with all parameters
            stream_hash = Stream.generate_hash_key(
                name, url, tvg_id, hash_keys, m3u_id=account_id, group=group_title,
                account_type=account_type_for_hash, stream_id=provider_stream_id
            )

            _attrs = stream_info["attributes"]
            _tv_archive_m3u = str(_attrs.get("tv_archive", "0"))
            _is_catchup_m3u = _tv_archive_m3u in ("1", "True")
            try:
                _catchup_days_m3u = int(_attrs.get("tv_archive_duration", 0) or 0)
            except (TypeError, ValueError):
                _catchup_days_m3u = 0
            _is_radio_m3u = (
                str(get_case_insensitive_attr(_attrs, "radio", "")).lower() in ("1", "true")
                or str(_attrs.get("stream_type", "")).lower() == "radio_streams"
            )

            stream_props = {
                "name": name,
                "url": url,
                "logo_url": tvg_logo,
                "tvg_id": tvg_id,
                "m3u_account": account,
                "channel_group_id": int(groups.get(group_title)),
                "stream_hash": stream_hash,
                "custom_properties": {**_attrs, "vlc_opts": stream_info["vlc_opts"]} if "vlc_opts" in stream_info else _attrs,
                "is_adult": parse_is_adult(_attrs.get("is_adult", 0)),
                "is_stale": False,
                "stream_id": provider_stream_id,
                "stream_chno": channel_num,
                "is_catchup": _is_catchup_m3u,
                "catchup_days": _catchup_days_m3u,
                "is_radio": _is_radio_m3u,
            }

            if stream_hash not in stream_hashes:
                stream_hashes[stream_hash] = stream_props
        except Exception as e:
            logger.error(f"Failed to process stream {name}: {e}")
            logger.error(json.dumps(stream_info))

    existing_streams = {
        s.stream_hash: s
        for s in Stream.objects.filter(stream_hash__in=stream_hashes.keys()).select_related('m3u_account').only(
            'id', 'stream_hash', 'name', 'url', 'logo_url', 'tvg_id', 'custom_properties', 'last_seen', 'updated_at', 'm3u_account', 'stream_id', 'stream_chno', 'channel_group_id', 'is_catchup', 'catchup_days', 'is_radio'
        )
    }

    for stream_hash, stream_props in stream_hashes.items():
        if stream_hash in existing_streams:
            obj = existing_streams[stream_hash]
            # Optimized field comparison
            changed = (
                obj.name != stream_props["name"] or
                obj.url != stream_props["url"] or
                obj.logo_url != stream_props["logo_url"] or
                obj.tvg_id != stream_props["tvg_id"] or
                obj.custom_properties != stream_props["custom_properties"] or
                obj.is_adult != stream_props["is_adult"] or
                obj.stream_id != stream_props["stream_id"] or
                obj.stream_chno != stream_props["stream_chno"] or
                obj.channel_group_id != stream_props["channel_group_id"] or
                obj.is_catchup != stream_props["is_catchup"] or
                obj.catchup_days != stream_props["catchup_days"] or
                obj.is_radio != stream_props["is_radio"]
            )

            obj.last_seen = timezone.now()
            obj.is_stale = False

            if changed:
                obj.name = stream_props["name"]
                obj.url = stream_props["url"]
                obj.logo_url = stream_props["logo_url"]
                obj.tvg_id = stream_props["tvg_id"]
                obj.custom_properties = stream_props["custom_properties"]
                obj.is_adult = stream_props["is_adult"]
                obj.stream_id = stream_props["stream_id"]
                obj.stream_chno = stream_props["stream_chno"]
                obj.channel_group_id = stream_props["channel_group_id"]
                obj.is_catchup = stream_props["is_catchup"]
                obj.catchup_days = stream_props["catchup_days"]
                obj.is_radio = stream_props["is_radio"]
                obj.updated_at = timezone.now()
                streams_to_update.append(obj)
            else:
                streams_to_touch.append(obj)
        else:
            # New stream
            stream_props["last_seen"] = timezone.now()
            stream_props["updated_at"] = timezone.now()
            stream_props["is_stale"] = False
            streams_to_create.append(Stream(**stream_props))

    try:
        with transaction.atomic():
            if streams_to_create:
                Stream.objects.bulk_create(streams_to_create, ignore_conflicts=True)

            _bulk_update_stream_refresh_batches(
                streams_to_update, streams_to_touch, batch_size=200,
            )
    except Exception as e:
        # The batch rolled back, so none of its streams got a fresh last_seen;
        # the refresh must not treat it as processed.
        logger.error(f"Bulk operation failed: {str(e)}")
        connections.close_all()
        raise

    retval = (
        f"M3U account: {account_id}, Batch processed: "
        + _batch_stream_count_message(
            len(streams_to_create),
            len(streams_to_update),
            len(streams_to_touch),
        )
    )

    # Clean up database connections for threading
    connections.close_all()

    # Free batch data structures (reference-counted deallocation)
    del streams_to_create, streams_to_update, streams_to_touch, stream_hashes, existing_streams
    gc.collect()

    return retval
