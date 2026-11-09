"""Fixture-only class configuration; the archived application source is unchanged."""
import os

if os.environ.get('POSTGRES_PASSWORD') != 'playback-fixture-only':
    raise RuntimeError('Cold configuration is restricted to the isolated fixture')
seconds = float(os.environ['COLD_GHOST_SECONDS'])
if not 1 <= seconds <= 30:
    raise ValueError('Ghost fixture must be1..30seconds')
from apps.proxy.config import TSConfig
TSConfig.CLIENT_HEARTBEAT_INTERVAL = 1
TSConfig.GHOST_CLIENT_MULTIPLIER = seconds
