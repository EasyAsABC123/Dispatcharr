



























































































































































































































































































































































@shared_task
def match_selected_channels_epg(channel_ids):
    """
    Match EPG data for only the specified selected channels.
    Uses the same integrated EPG matching logic but processes only selected channels.
    """
    try:
        logger.info(f"Starting integrated EPG matching for {len(channel_ids)} selected channels...")

        # Get region preference
        try:
            region_obj = CoreSettings.objects.get(key="preferred-region")
            region_code = region_obj.value.strip().lower()
        except CoreSettings.DoesNotExist:
            region_code = None

        # Selected-channel matching always runs, including channels that already have EPG.
        selected_channels = Channel.objects.filter(id__in=channel_ids)
        logger.info(f"Processing {selected_channels.count()} selected channel(s) for EPG matching")

        if not selected_channels.exists():
            logger.info("No selected channels found for EPG matching.")

            from core.utils import send_websocket_update

            send_websocket_update(
                'updates',
                'update',
                {
                    "success": True,
                    "type": "epg_match",
                    "refresh_channels": True,
                    "matches_count": 0,
                    "message": "No selected channels found for EPG matching",
                    "associations": [],
                },
            )
            return "No selected channels found for EPG matching."

        channels_data = []
        for channel in selected_channels:
            normalized_tvg_id = channel.tvg_id.strip().lower() if channel.tvg_id else ""
            normalized_gracenote_id = channel.tvc_guide_stationid.strip().lower() if channel.tvc_guide_stationid else ""
            channels_data.append({
                "id": channel.id,
                "name": channel.name,
                "tvg_id": normalized_tvg_id,
                "original_tvg_id": channel.tvg_id,
                "gracenote_id": normalized_gracenote_id,
                "original_gracenote_id": channel.tvc_guide_stationid,
                "fallback_name": normalized_tvg_id if normalized_tvg_id else channel.name,
                "norm_chan": normalize_name(channel.name),
                "current_epg_data_id": channel.epg_data_id,
            })

        epg_data, epg_tvg_id_index = build_epg_matching_catalog()
        logger.info(f"Processing {len(channels_data)} selected channels against {len(epg_data)} EPG entries (from active sources only)")

        # Run EPG matching with progress updates - automatically uses appropriate thresholds
        result = match_channels_to_epg(
            channels_data,
            epg_data,
            region_code,
            use_ml=True,
            send_progress=True,
            epg_tvg_id_index=epg_tvg_id_index,
        )
        channels_to_update_dicts = result["channels_to_update"]
        matched_channels = result["matched_channels"]
        unchanged_channels = result.get("unchanged_channels", [])

        changed_associations = apply_matched_epg_to_channels(channels_to_update_dicts)

        channels_updated = len(changed_associations)
        if channels_updated:
            logger.info(
                f"Selected Channel Match Summary: {channels_updated} channel(s) updated."
            )
            for (cid, cname, tvg) in matched_channels:
                logger.info(f"  - Channel '{cname}' (id={cid}) => tvg_id={tvg!r}")
        if unchanged_channels:
            logger.debug(
                f"{len(unchanged_channels)} selected channel(s) already correctly matched "
                f"(unchanged)"
            )
        if not channels_updated and not unchanged_channels:
            logger.info("No selected channels were matched.")

        logger.info("Finished integrated EPG matching for selected channels.")

        from core.utils import send_websocket_update

        if channels_updated:
            match_message = (
                f"EPG matching complete: {channels_updated} selected channel(s) updated"
            )
        elif unchanged_channels:
            match_message = (
                f"EPG matching complete: {len(unchanged_channels)} selected channel(s) "
                f"already correctly matched"
            )
        else:
            match_message = "EPG matching complete: no matches found"

        send_websocket_update(
            'updates',
            'update',
            {
                "success": True,
                "type": "epg_match",
                "refresh_channels": True,
                "matches_count": channels_updated,
                "message": match_message,
                "associations": changed_associations,
            },
        )

        return (
            f"Done. {channels_updated} selected channel(s) updated "
            f"({len(unchanged_channels)} unchanged)."
        )

    finally:
        cleanup_after_matching()
        from core.utils import cleanup_memory
        cleanup_memory(log_usage=True, force_collection=True)
