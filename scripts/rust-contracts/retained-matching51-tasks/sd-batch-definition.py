@shared_task(
    name='apps.epg.tasks.fetch_sd_mapped_guide_batch',
    time_limit=3600,
    soft_time_limit=3500,
)
def fetch_sd_mapped_guide_batch(source_id, force=False, _defer_retry=0):
    """
    Fetch Schedules Direct guide data for all mapped stations on one source.

    Used when bulk EPG assignment would otherwise queue many per-EPG tasks.
    """
    try:
        source = EPGSource.objects.get(id=source_id)
    except EPGSource.DoesNotExist:
        logger.error(f"EPGSource {source_id} not found for SD mapped guide batch")
        return

    if source.source_type != 'schedules_direct':
        return "Not a Schedules Direct source"

    if not acquire_task_lock('sd_mapped_guide_fetch', source_id):
        if _defer_retry < SD_MAPPED_GUIDE_FETCH_DEFER_MAX_RETRIES:
            logger.info(
                f"SD mapped guide batch for source {source_id} already in progress, "
                f"deferring retry {_defer_retry + 1}/"
                f"{SD_MAPPED_GUIDE_FETCH_DEFER_MAX_RETRIES}"
            )
            fetch_sd_mapped_guide_batch.apply_async(
                args=[source_id],
                kwargs={
                    'force': force,
                    '_defer_retry': _defer_retry + 1,
                },
                countdown=SD_MAPPED_GUIDE_BATCH_DEFER_SECONDS,
            )
            return "Deferred - batch already in progress"
        logger.warning(
            f"SD mapped guide batch for source {source_id} still locked after "
            f"{_defer_retry} deferrals; giving up"
        )
        return "Task already running"

    lock_renewer = TaskLockRenewer('sd_mapped_guide_fetch', source_id)
    lock_renewer.start()
    try:
        logger.info(f"Fetching Schedules Direct guide for mapped stations (source: {source.name})")
        fetch_schedules_direct(source, mapped_guide_batch=True, force=force)
        return "SD mapped guide batch complete"
    finally:
        lock_renewer.stop()
        release_task_lock('sd_mapped_guide_fetch', source_id)
