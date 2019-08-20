#! /usr/bin/env python3

import os, shutil

# Run testing
os.chdir('tests')
os.system('pytest -vv -s $@')

# Delete a garbage data
os.chdir(os.path.pardir)

