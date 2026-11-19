"""Isolated existing-schema Connect seed and row snapshots in the Original role."""
from apps.connect.models import DeliveryLog, EventSubscription, Integration
from django.db import transaction

from connect_lifecycle_fixture import snapshot
from connect_matrix_contract import MUTATIONS, PREFIX, RECEIVER, ROLES, TABLES, cell, phase


def seed_group(name,*,create_subscription,with_log):
    integration=Integration.objects.create(name=name,type='webhook',config={'url':RECEIVER},enabled=False)
    identity={'integration':integration.pk}
    if create_subscription:
        subscription=EventSubscription.objects.create(integration=integration,event='vod_start',enabled=True,
                                                     payload_template='owned-connect-matrix')
        identity['subscription']=subscription.pk
        if with_log:
            log=DeliveryLog.objects.create(subscription=subscription,status='success',request_payload={'owned':True})
            identity['log']=log.pk
    return identity


def prepare(side,variant):
    prefix=phase(side,variant)
    if Integration.objects.filter(name__startswith=PREFIX).exists():
        raise ValueError('Prior disposable matrix phase must be retired')
    before=snapshot()
    selection={'read':seed_group(prefix+'read',create_subscription=True,with_log=True)}
    for key in sorted(MUTATIONS):
        for role in ROLES:
            name=cell(key,role)
            selection[name]=seed_group(prefix+name,create_subscription=key!='subscription-create',
                                       with_log=key=='subscription-delete')
    seeded=snapshot()
    if any(row not in seeded[table] for table in TABLES for row in before[table]):
        raise ValueError('Matrix fixture setup changed a retained row')
    return {'before':before,'seeded':seeded,'selected':selection}


def retire(side,variant):
    prefix=phase(side,variant)
    owned=Integration.objects.filter(name__startswith=prefix)
    count=owned.count()
    if count!=1+len(MUTATIONS)*len(ROLES):
        raise ValueError('Unexpected integration ownership at cleanup')
    owned.delete()
    if Integration.objects.filter(name__startswith=PREFIX).exists():
        raise ValueError('Retained matrix integration after retirement')
    return snapshot()


def dispatch(data):
    action=data['action']
    if action=='connect_matrix_snapshot':
        return snapshot()
    with transaction.atomic():
        if action=='connect_matrix_prepare':
            return prepare(data['side'],data['variant'])
        if action=='connect_matrix_retire':
            return retire(data['side'],data['variant'])
        raise ValueError('Unexpected bounded Connect matrix operation')
