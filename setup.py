# -*- coding: utf-8 -*-

import os

from os.path import dirname, join
from setuptools import setup, find_packages, Command

# Hack because logging + setuptools sucks.
import multiprocessing

with open('README.rst') as f:
    readme = f.read()

setup(
    name = 'oauthlib',
    version = '0.0.1',
    description = 'Python implementation of OAuth 1.0a',
    long_description = fread('README.rst'),
    author = '',
    author_email = '',
    url = 'https://github.com/idangazit/oauthlib',
    license = fread('LICENSE'),
    packages = find_packages(exclude=('tests', 'docs')),
    test_suite = 'nose.collector',
    tests_require=tests_require,
    extras_require={'test': tests_require},
)

