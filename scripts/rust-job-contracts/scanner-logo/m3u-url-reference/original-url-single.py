@shared_task(time_limit=3600, soft_time_limit=3500)
def refresh_single_m3u_account(account_id):
    """Splits M3U processing into chunks and dispatches them as parallel tasks."""
    if not acquire_task_lock("refresh_single_m3u_account", account_id):
        return f"Task already running for account_id={account_id}."

    # Keep the lock alive while this long-running task is working.
    # Without renewal, the 300s lock TTL can expire during large
    # downloads/parses, allowing duplicate tasks to start.
    lock_renewer = TaskLockRenewer("refresh_single_m3u_account", account_id)
    lock_renewer.start()

    _release_task_db_connection()

    try:
        return _refresh_single_m3u_account_impl(account_id)
    except Exception as e:
        logger.error(
            f"refresh_single_m3u_account failed for account {account_id}: {e}",
            exc_info=True,
        )
        account_name = None
        try:
            account_name = (
                M3UAccount.objects.filter(id=account_id)
                .values_list("name", flat=True)
                .first()
            )
        except Exception:
            pass
        _set_m3u_account_status(
            account_id,
            M3UAccount.Status.ERROR,
            f"Error processing M3U: {str(e)[:500]}",
            account_name=account_name,
            notify_error=True,
            ws_error=str(e)[:500],
        )
    finally:
        _ensure_m3u_refresh_terminal_status(account_id)
        _release_task_db_connection()
        lock_renewer.stop()
        release_task_lock("refresh_single_m3u_account", account_id)
