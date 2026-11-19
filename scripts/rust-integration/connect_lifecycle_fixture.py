"""Database observations and inert log setup for the Connect lifecycle suite."""

from apps.connect.models import DeliveryLog, EventSubscription, Integration
from django.db import connection, transaction


PREFIX = 'rust-probe-connect-lifecycle-'


def snapshot():
    integrations = list(Integration.objects.order_by('id').values(
        'id', 'name', 'type', 'config', 'enabled', 'created_at'))
    if any(not row['name'].startswith('rust-probe-connect-') for row in integrations):
        raise RuntimeError('Connect lifecycle snapshots require an owned fixture database')
    subscriptions = list(EventSubscription.objects.order_by('id').values(
        'id', 'integration_id', 'event', 'enabled', 'payload_template'))
    logs = list(DeliveryLog.objects.order_by('id').values(
        'id', 'subscription_id', 'status', 'request_payload', 'response_payload', 'error_message', 'created_at'))
    for row in integrations + logs:
        row['created_at'] = row['created_at'].isoformat().replace('+00:00', 'Z')
    for row in subscriptions:
        row['integration'] = row.pop('integration_id')
    tables = {'integrations': 'integration', 'subscriptions': 'eventsubscription', 'logs': 'deliverylog'}
    sequences = {}
    with connection.cursor() as cursor:
        for key, table in tables.items():
            cursor.execute(f'SELECT last_value, is_called FROM dispatcharr_connect_{table}_id_seq')
            sequences[key] = list(cursor.fetchone())
    return {'integrations': integrations, 'subscriptions': subscriptions, 'logs': logs, 'sequences': sequences}


def dispatch(request):
    with transaction.atomic():
        if request['action'] == 'connect_lifecycle_seed_sibling':
            if Integration.objects.filter(name__startswith=PREFIX).exists():
                raise RuntimeError('Connect lifecycle already used this project')
            sibling = Integration.objects.create(name=PREFIX + 'sibling', type='webhook', enabled=False)
            subscription = EventSubscription.objects.create(
                integration=sibling, event='vod_start', enabled=False, payload_template='sibling')
            log = DeliveryLog.objects.create(subscription=subscription, status='success')
            return {'integration': sibling.pk, 'subscription': subscription.pk, 'log': log.pk}
        if request['action'] == 'connect_lifecycle_seed_logs':
            name = request['name']
            if name not in {PREFIX + 'source', PREFIX + 'native'}:
                raise RuntimeError('Connect lifecycle log seed requires a known workflow name')
            integration = Integration.objects.get(pk=request['integration'], name=name)
            recording = integration.subscriptions.get(event='recording_end')
            if (integration.subscriptions.filter(event='download_complete').exists()
                    or DeliveryLog.objects.filter(subscription__integration=integration).exists()):
                raise RuntimeError('Connect lifecycle log seed requires a fresh stock subscription set')
            # The first migration admitted download_complete. It can still
            # exist in persisted rows, but the stock form no longer sends it.
            legacy = EventSubscription.objects.create(
                integration=integration, event='download_complete', enabled=False)
            logs = DeliveryLog.objects.bulk_create([
                DeliveryLog(subscription=row, status='success', request_payload={'fixture': row.event})
                for row in (recording, legacy)
            ])
            return {'subscription': legacy.pk, 'logs': [row.pk for row in logs]}
        if request['action'] == 'connect_lifecycle_snapshot':
            return snapshot()
        raise RuntimeError('Unexpected Connect lifecycle fixture operation')
