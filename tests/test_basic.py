# -*- coding: utf-8 -*-

# first option to import via context.py
# from .context import sample

# second option to import the module directly since it has __init__.py
import sample

import unittest


class BasicTestSuite(unittest.TestCase):
    """Basic test cases."""

    def test_absolute_truth_and_meaning(self):
        assert True


if __name__ == '__main__':
    unittest.main()
