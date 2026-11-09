"""Reuse the fixture web supervisor with an explicit configuration-only preload."""
import sys
sys.path.insert(0, '/verification')
import supervisor
supervisor.SOCKET = '/tmp/dispatcharr-cold-web.sock'
OriginalWebProcess = supervisor.WebProcess


class ConfiguredWebProcess(OriginalWebProcess):
    def __init__(self, command):
        super().__init__(['/dispatcharrpy/bin/uwsgi', *command[1:], '--pythonpath', '/integration', '--import', 'cold_config'])


supervisor.WebProcess = ConfiguredWebProcess
if __name__ == '__main__':
    if sys.argv[1:] == ['serve']:
        supervisor.serve()
    else:
        raise SystemExit(supervisor.control(sys.argv[1]))
