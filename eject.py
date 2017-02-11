#!/usr/bin/env python
from __future__ import print_function
import os
import sys
import re
import shutil

import inquirer
import fileinput

nonempty = lambda _, x: re.match('.+', x)
questions = [
    inquirer.Text(
        'sample', 'Name of your project?', default='sample',
        validate=nonempty),
    inquirer.Text(
        'kennethreitz/samplemod',
        'Github slug?',
        default='kennethreitz/samplemod',
        validate=nonempty),
    inquirer.Text(
        'Kenneth Reitz',
        'Your name?',
        default="Kenneth Reitz",
        validate=nonempty),
    inquirer.Text(
        'me@kennethreitz.com',
        'Your email?',
        default="me@kennethreitz.com",
        validate=nonempty),
]

replacemap = inquirer.prompt(questions)
changed = {k: v for k, v in replacemap.items() if k != v}
files = filter(lambda x: x != '.gitignore' and x != 'eject.py',
               os.popen('git ls-files').read().split())

for f in files:
    filedata = None
    with open(f, 'r') as file:
        filedata = file.read()

    for (key, value) in filter(lambda (k, _): k != 'sample', changed.items()):
        filedata = filedata.replace(key, value)

    if changed.get('sample'):
        filedata = filedata.replace('sample', changed['sample'])
        filedata = filedata.replace('Sample', changed['sample'].capitalize())

    with open(f, 'w') as file:
        file.write(filedata)

if changed.get('sample'):
    shutil.move('sample', changed['sample'])

shutil.rmtree('.git')
os.remove('eject.py')
