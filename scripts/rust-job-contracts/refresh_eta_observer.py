"""Owned refresh ETA observer; original task implementations are unchanged.
Original Dispatcharr/Celery/Kombu authors retain all code/protocol credit.
Fixture-only route control keeps Index/Series descendants unconsumed.
"""
from epg_refresh_observer import *
app.conf.task_routes={
 'apps.epg.tasks.build_programme_index_task':{'queue':'rust-refresh-index-control'},
 'apps.channels.tasks.evaluate_series_rules':{'queue':'rust-refresh-series-control'},
}
