def log_system_event(event_type, channel_id=None, channel_name=None, **details):
    """
    Log a system event and maintain the configured max history.

    Args:
        event_type: Type of event (e.g., 'channel_start', 'client_connect')
        channel_id: Optional UUID of the channel
        channel_name: Optional name of the channel
        **details: Additional details to store in the event (stored as JSON)

    Example:
        log_system_event('channel_start', channel_id=uuid, channel_name='CNN',
                        stream_url='http://...', user='admin')
    """
    from core.models import SystemEvent, CoreSettings
    from django.db import close_old_connections

    try:
        # Create the event
        SystemEvent.objects.create(
            event_type=event_type,
            channel_id=channel_id,
            channel_name=channel_name,
            details=details
        )

        # Connect integrations and plugin event hooks (non-blocking on gevent uWSGI)
        _dispatch_system_event_integrations(
            event_type,
            channel_id=channel_id,
            channel_name=channel_name,
            **details,
        )

        # Get max events from settings (default 100)
        try:
            from .models import CoreSettings
            max_events = int(
                CoreSettings.get_system_settings().get("max_system_events", 100) or 100
            )
        except Exception:
            max_events = 100

        # Delete old events beyond the limit (keep it efficient with a single query)
        total_count = SystemEvent.objects.count()
        if total_count > max_events:
            # Get the ID of the event at the cutoff point
            cutoff_event = SystemEvent.objects.values_list('id', flat=True)[max_events]
            # Delete all events with ID less than cutoff (older events)
            SystemEvent.objects.filter(id__lt=cutoff_event).delete()

    except Exception as e:
        # Don't let event logging break the main application
        logger.error(f"Failed to log system event {event_type}: {e}")
    finally:
        # geventpool keeps checked-out connections until close(); release promptly
        # when logging from proxy greenlets/threads outside a normal request cycle.
        if _is_gevent_monkey_patched():
            close_old_connections()
