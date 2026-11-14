"""Pure owned-isolation acceptors; no Internet reachability probes."""
import collections,datetime,hashlib,ipaddress,json,math,socket,struct
NETWORK=ipaddress.ip_network('11.253.250.0/29');SOURCE='11.253.250.3';ORIGIN='11.253.250.2'
def need(ok,label):
 if not ok:raise ValueError(label)
def host_nonoverlap(routes,networks):
 for row in routes:
  dst=row.get('dst')
  if dst in(None,'default','0.0.0.0/0'):continue
  try:existing=ipaddress.ip_network(dst,strict=False)
  except ValueError:raise ValueError('ExplicitKnownIPv4Route')
  need(not existing.overlaps(NETWORK),'OwnedIPAMOverlapsExistingHostSpecificRoute')
 no_l3=[]
 for network in networks:
  need(type(network)is dict and type(network.get('IPAM'))is dict and 'Config'in network['IPAM'],'ExplicitDockerIPAMMetadataRequired');configs=network['IPAM']['Config']
  if configs is None or configs==[]:
   # Docker's two built-in local non-L3 networks have literal Config:null.
   # Do not turn missing/unknown bridge metadata into an empty overlap check.
   need((network.get('Name'),network.get('Driver'))in(('host','host'),('none','null'))and network.get('Scope')=='local'and network['IPAM'].get('Driver')=='default','NullOrEmptyIPAMOnlyRecognizedBuiltinHostNone');no_l3.append({'Name':network['Name'],'Driver':network['Driver'],'literal_Config':configs});continue
  need(type(configs)is list and len(configs)<=256,'BoundedExplicitDockerIPAMList')
  for config in configs:
   need(type(config)is dict and type(config.get('Subnet'))is str and bool(config['Subnet']),'ExplicitDockerSubnetRequired');subnet=ipaddress.ip_network(config['Subnet'])
   if subnet.version==4:need(not subnet.overlaps(NETWORK),'OwnedIPAMOverlapsExistingDockerNetwork')
 return {'passed':True,'planned':str(NETWORK),'specific_host_routes_checked':len(routes),'existing_Docker_networks_checked':len(networks),'recognized_builtin_no_L3_IPAM':no_l3,'no_live_Internet_probe':True}
def actual_network(net,source,origin,project):
 name=project+'_owned';need(net['Name']==name and net['Internal']is True and net.get('EnableIPv6')is False,'ExactOwnedInternalIPv4OnlyNetwork');need(net.get('Labels',{}).get('com.docker.compose.project')==project,'ExactOwnedNetworkLabel');need(net['Driver']=='bridge'and net.get('Options',{}).get('com.docker.network.bridge.enable_ip_masquerade')=='false','ExplicitNoIPMasquerade');need(net['IPAM']['Config']==[{'Subnet':str(NETWORK),'Gateway':'11.253.250.1'}],'ExactIPAMOnlyInternalGateway');need(set(net['Containers'])=={source['Id'],origin['Id']},'OnlyTwoDeclaredOwnedEndpoints')
 for container,address in [(source,SOURCE),(origin,ORIGIN)]:
  need(container['Config']['Labels'].get('com.docker.compose.project')==project,'OwnedContainerLabel');need(set(container['NetworkSettings']['Networks'])=={name}and container['HostConfig']['NetworkMode']==name,'NoExtraNATOrOtherNetwork');need(container['HostConfig'].get('Dns')==['127.0.0.1'],'NoInheritedExternalDNSUpstream');need(not container['HostConfig'].get('PortBindings')and all(v is None for v in container['NetworkSettings'].get('Ports',{}).values()),'NoPublishedPorts');endpoint=container['NetworkSettings']['Networks'][name];need(endpoint['IPAddress']==address and endpoint['NetworkID']==net['Id'],'ExactOwnedEndpointIP')
  env=dict(item.split('=',1)for item in container['Config']['Env']if'='in item)
  for key in ['HTTP_PROXY','HTTPS_PROXY','ALL_PROXY','http_proxy','https_proxy','all_proxy']:need(env.get(key)=='','NoInheritedProxy:'+key)
  need(env.get('NO_PROXY')==env.get('no_proxy')=='*','LiteralNoProxyAll')
 return {'passed':True,'network':name,'only_internal_endpoints':[SOURCE,ORIGIN],'no_ports_or_extra_network_or_proxy':True,'no_external_gateway_claim_requires_actual_kernel_routes':True}
def kernel_routes(ipv4,ipv6):
 four=[]
 for line in ipv4.splitlines()[1:]:
  fields=line.split();need(len(fields)>=11,'KernelIPv4RouteFields');iface,dst,gateway,flags,_,_,metric,mask=fields[:8];destination=socket.inet_ntoa(struct.pack('<I',int(dst,16)));via=socket.inet_ntoa(struct.pack('<I',int(gateway,16)));four.append({'interface':iface,'destination':destination,'gateway':via,'mask_hex':mask,'flags_hex':flags,'metric':metric});need(not(dst=='00000000'and mask=='00000000'and iface!='lo'),'NoExternalIPv4DefaultRoute');need(iface=='lo'or(ipaddress.ip_network(destination+'/'+socket.inet_ntoa(struct.pack('<I',int(mask,16))),strict=False).subnet_of(NETWORK)and via=='0.0.0.0'),'OnlyOwnedConnectedIPv4Routes')
 six=[]
 for line in ipv6.splitlines():
  f=line.split();need(len(f)>=10,'KernelIPv6RouteFields');six.append(f);need(not(f[0]=='0'*32 and f[1]=='00'and f[-1]!='lo'),'NoExternalIPv6DefaultRoute');need(f[-1]=='lo','NoNonLoopbackIPv6Route')
 return {'passed':True,'actual_ipv4_routes':four,'actual_ipv6_routes':six,'loopback_unreachable_default_is_not_external_path':True}
