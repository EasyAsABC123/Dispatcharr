"""Offline shared-deadline/cleanup tests; no runtime acceptance evidence."""
import unittest
from unittest.mock import patch
from budget import LeaseBudget,LeaseExpired
class DeadlineRegression(unittest.TestCase):
 def test_deadline_does_not_reset_for_a_second_backend(self):
  with patch('budget.time.monotonic',return_value=10):first=LeaseBudget(100);self.assertEqual(first.remaining(200),90)
  with patch('budget.time.monotonic',return_value=99):second=LeaseBudget(100);self.assertEqual(second.remaining(200),1)
  with patch('budget.time.monotonic',return_value=100):
   for item in(first,second):
    with self.assertRaises(LeaseExpired):item.remaining(1)
 def test_cleanup_is_one_separate_90_second_budget(self):
  with patch('budget.time.monotonic',return_value=100):item=LeaseBudget(100);item.begin_cleanup();self.assertEqual(item.remaining(1000),90)
  with patch('budget.time.monotonic',return_value=140):item.begin_cleanup();self.assertEqual(item.remaining(1000),50);self.assertTrue(item.expired())
  with patch('budget.time.monotonic',return_value=190):
   with self.assertRaises(LeaseExpired):item.remaining(1)
 def test_deadline_is_required_and_finite(self):
  for value in(True,None,float('inf'),float('nan')):
   with self.assertRaises(ValueError):LeaseBudget(value)
if __name__=='__main__':unittest.main()
