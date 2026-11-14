"""Only this fixture's connected IPv4 loopback TCP sockets; no production change."""
import socket,time
ROLES={'native-downstream','postgres-upstream','redis-downstream','redis-upstream'}
def configure(sock,role):
 if role not in ROLES:raise ValueError('ExactOwnedRelaySocketRole')
 if sock.family!=socket.AF_INET or sock.type!=socket.SOCK_STREAM:raise ValueError('OwnedIPv4TCPStream')
 peer=sock.getpeername();local=sock.getsockname()
 if peer[0]!='127.0.0.1'or local[0]!='127.0.0.1':raise ValueError('OwnedLoopbackSocketOnly')
 before=sock.getsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY);sock.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1);after=sock.getsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY)
 if type(before)is not int or before not in(0,1)or type(after)is not int or after!=1:raise ValueError('ActualTCP_NODELAYAcknowledgement')
 return {'role':role,'family':int(sock.family),'type':int(sock.type),'local':list(local),'peer':list(peer),'actual_TCP_NODELAY_before':before,'actual_TCP_NODELAY_after':after,'observed_monotonic':time.monotonic(),'scope':'owned fixture socket only; does not measure latency or retroactively explain old timings'}
