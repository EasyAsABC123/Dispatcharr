"""New StreamProfile input/substitution cases; never executed by local import."""
URL='http://owned.invalid/media';AGENT='Owned Agent/1.0';CHANNEL='0bd6295a-8e4a-4fa4-afd3-5df8f8aebd30'
def cases():
 def row(name,parameters='-i {streamUrl}',command='ffmpeg',url=URL,agent=AGENT,channel=CHANNEL,locked=False,proxy_name=False):return {'name':name,'command':command,'parameters':parameters,'stream_url':url,'user_agent':agent,'channel_id':channel,'locked':locked,'use_Source_Proxy_name':proxy_name}
 return [
  row('locked-Proxy-short-circuit',parameters="'unterminated",agent=None,locked=True,proxy_name=True),
  row('unlocked-Proxy-is-command',proxy_name=True),
  row('FFmpeg-quoted-placeholders',parameters='-user_agent "{userAgent}" -i "{streamUrl}" -metadata channel="{channelId}" -f mpegts pipe:1'),
  row('VLC-command-literal',command='vlc',parameters='-I dummy "{streamUrl}" --http-user-agent="{userAgent}" --sout "#std{access=file,mux=ts,dst=-}"'),
  row('insert-spaces-and-shell-data',parameters='-i "{streamUrl}" -metadata "agent={userAgent}"',url='http://owned.invalid/two words;$HOME',agent='Owned "quoted" ; $(false)'),
  row('replacement-order-cascade',parameters='{streamUrl} {userAgent} {channelId}',url='http://owned.invalid/{userAgent}',agent='owned-{channelId}-{streamUrl}'),
  row('channel-None',parameters='id={channelId}',channel=None),
  row('channel-int-zero',parameters='id={channelId}',channel=0),
  row('channel-string-zero',parameters='id={channelId}',channel='0'),
  row('channel-UUID',parameters='id={channelId}'),
  row('command-space-not-split',command='ffmpeg -nostdin'),
  row('repeat-placeholders',parameters='{streamUrl}|{streamUrl}|{userAgent}|{userAgent}'),
  row('outer-braces',parameters='{{streamUrl}} {{channelId}}'),
  row('empty-parameters',parameters=''),
  row('Unicode-values',url='http://owned.invalid/é/東京',agent='Owned α Agent'),
  row('unclosed-quote',parameters='-i "{streamUrl}'),
  row('None-agent-not-coerced',agent=None),
  row('NUL-is-recorded-never-executed',url='http://owned.invalid/a\x00b'),
 ]
if __name__=='__main__':
 import json;print(json.dumps({'protocol':'Source-StreamProfile18-owner6','cases':cases(),'no_process_or_network_delivery':True},indent=2))
