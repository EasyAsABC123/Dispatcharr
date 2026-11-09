import unittest
from run import rootless_port_collision
class StartupRetryClassification(unittest.TestCase):
    def test_only_exact_rootless_bind_collision_is_retryable(self):
        self.assertTrue(rootless_port_collision('RootlessKit PortManager.AddPort(): listen tcp4 127.0.0.1:33006: bind: address already in use'))
        for text in [None,'bind: address already in use','RootlessKit PortManager.AddPort(): permission denied','database startup failed','provider HTTP500']:
            self.assertFalse(rootless_port_collision(text))
if __name__=='__main__':unittest.main()
