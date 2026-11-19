"""The bounded method, identity and role roster for the Connect matrix."""
from dataclasses import dataclass


PREFIX='rust-probe-connect-matrix-'
RECEIVER='http://origin:8080/owned-connect-create-webhook'
ROLES=('anonymous','inactive_admin','viewer','standard','admin_jwt','admin_api_key')
SIDES=('source','native')
VARIANTS=('bare','json')
DENIALS={'anonymous':401,'inactive_admin':401,'viewer':403,'standard':403}
ALLOW={'admin_jwt','admin_api_key'}


@dataclass(frozen=True)
class Route:
    key: str
    method: str
    path: str
    bare: int
    formatted: int
    status: int


ROUTES=(
    Route('integration-get','GET','/api/connect/integrations/{integration}/',334,335,200),
    Route('nested-subscriptions','GET','/api/connect/integrations/{integration}/subscriptions/',336,337,200),
    Route('subscriptions-list','GET','/api/connect/subscriptions/',342,343,200),
    Route('subscription-detail','GET','/api/connect/subscriptions/{subscription}/',344,345,200),
    Route('log-detail','GET','/api/connect/logs/{log}/',348,349,200),
    Route('integration-patch','PATCH','/api/connect/integrations/{integration}/',334,335,200),
    Route('subscription-create','POST','/api/connect/subscriptions/',342,343,201),
    Route('subscription-put','PUT','/api/connect/subscriptions/{subscription}/',344,345,200),
    Route('subscription-patch','PATCH','/api/connect/subscriptions/{subscription}/',344,345,200),
    Route('subscription-delete','DELETE','/api/connect/subscriptions/{subscription}/',344,345,204),
)
MUTATIONS={route.key for route in ROUTES if route.method!='GET'}
TABLES=('integrations','subscriptions','logs')
BY_KEY={route.key:route for route in ROUTES}


def cell(key,role):
    return key+'-'+role


def phase(side,variant):
    if side not in SIDES or variant not in VARIANTS:
        raise ValueError('Unexpected disposable phase')
    return PREFIX+side+'-'+variant+'-'


def route_path(route,variant,target):
    base=route.path.format(**target)
    return base if variant=='bare' else base.removesuffix('/')+'.json'


def symbolic_path(route,variant):
    return route.path if variant=='bare' else route.path.removesuffix('/')+'.json'


def payload(route,role,prefix,ids):
    tag=prefix+cell(route.key,role)
    if route.key=='integration-patch':
        return {'name':tag+'-changed'}
    if route.key=='subscription-create':
        return {'integration':ids['integration'],'event':'recording_end','enabled':False,'payload_template':tag}
    if route.key=='subscription-put':
        return {'integration':ids['integration'],'event':'channel_failover','enabled':False,'payload_template':tag+'-put'}
    if route.key=='subscription-patch':
        return {'enabled':False}
    return None
