# -*- coding: utf-8 -*-

# first option to import via context.py
# from .context import sample

# second option to import the module directly since it has __init__.py
import sample

import unittest


class AdvancedTestSuite(unittest.TestCase):
    """Advanced test cases."""

    def test_thoughts(self):
        sample.hmm()


if __name__ == '__main__':
    unittest.main()
