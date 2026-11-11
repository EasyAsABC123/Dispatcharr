# Read-only signals. Original Celery authors retain behavior/code credit.
from celery.signals import task_revoked,task_received
@task_revoked.connect
def expired_signal(sender=None,request=None,terminated=None,signum=None,expired=None,**kw):
 append('revoked',{'id':request.id,'task':sender.name,'expired':expired,'terminated':terminated,'signum':signum,'ack_exposed_on_context':hasattr(request,'acknowledged')})
@task_received.connect
def received_signal(request=None,**kw):
 append('received',{'id':request.id,'task':request.name,'expires':str(request.expires),'delivery_info':request.delivery_info})
