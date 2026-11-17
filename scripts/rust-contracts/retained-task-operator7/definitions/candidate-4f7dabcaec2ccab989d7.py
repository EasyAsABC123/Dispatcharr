






























































































@shared_task
def refresh_vod_content(account_id):
    """Refresh VOD content for an M3U account with batch processing for improved performance"""
    # Import here to avoid circular import
    from apps.m3u.tasks import send_m3u_update

    try:
        account = M3UAccount.objects.select_related("user_agent").get(id=account_id, is_active=True)

        if account.account_type != M3UAccount.Types.XC:
            logger.warning(f"VOD refresh called for non-XC account {account_id}")
            return "VOD refresh only available for XtreamCodes accounts"

        logger.info(f"Starting batch VOD refresh for account {account.name}")
        start_time = timezone.now()

        # Send start notification
        send_m3u_update(account_id, "vod_refresh", 0, status="processing")

        with XtreamCodesClient(
            account.server_url,
            account.username,
            account.password,
            account.get_user_agent_string()
        ) as client:

            category_maps = refresh_categories(account.id, client)
            if category_maps is None:
                message = (
                    f"Provider returned no VOD categories for account {account.name}; "
                    "aborting VOD refresh to preserve existing category selections"
                )
                logger.warning(message)
                send_m3u_update(account_id, "vod_refresh", 100, status="error",
                               message=f"VOD refresh failed: {message}")
                return f"VOD refresh failed: {message}"

            movie_categories, series_categories = category_maps

            logger.debug("Fetching relations for filtering category filtering")
            relations = { rel.category_id: rel for rel in M3UVODCategoryRelation.objects
                .filter(m3u_account=account)
                .select_related("category", "m3u_account")
            }

            # Refresh movies with batch processing (pass scan start time)
            refresh_movies(client, account, movie_categories, relations, scan_start_time=start_time)

            # Refresh series with batch processing (pass scan start time)
            refresh_series(client, account, series_categories, relations, scan_start_time=start_time)

        # Reconcile language once per account refresh, not per batch. Relations
        # stay pinned during the batch (see process_movie_batch /
        # process_series_batch). Must run before cleanup_orphaned_vod_content
        # so cleanup's orphan-deletion stays a backstop, not the primary mechanism.
        logger.info(f"Reconciling VOD language identity for account {account.name}")
        movie_summary = reconcile_movie_language_identity(account, scan_start_time=start_time)
        series_summary = reconcile_series_language_identity(account, scan_start_time=start_time)

        end_time = timezone.now()
        duration = (end_time - start_time).total_seconds()

        skip_note = _language_skip_note(movie_summary, series_summary)
        completion = f"Batch VOD refresh completed for account {account.name} in {duration:.2f} seconds{skip_note}"
        logger.info(completion)

        # Cleanup orphaned VOD content after refresh (scoped to this account only)
        logger.info(f"Starting cleanup of orphaned VOD content for account {account.name}")
        cleanup_result = cleanup_orphaned_vod_content(account_id=account_id, scan_start_time=start_time)
        logger.info(f"VOD cleanup completed: {cleanup_result}")

        # Send completion notification
        send_m3u_update(account_id, "vod_refresh", 100, status="success",
                       message=f"VOD refresh completed in {duration:.2f} seconds{skip_note}")

        return completion

    except Exception as e:
        import traceback
        logger.error(f"Error refreshing VOD for account {account_id}: {str(e)}")
        logger.error(f"Full traceback:\n{traceback.format_exc()}")

        # Send error notification
        send_m3u_update(account_id, "vod_refresh", 100, status="error",
                       message=f"VOD refresh failed: {str(e)}")

        return f"VOD refresh failed: {str(e)}"
