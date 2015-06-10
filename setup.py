#!/usr/bin/env python
import sys
from distutils.core import setup

sys.path.insert(0, '.')
import version

setup(name="zmq_helpers",
      version=version.getVersion(),
      description="zmq helper functions and classes",
      keywords="zmq helper",
      author="Christian Fobel",
      author_email="christian@fobel.net",
      url="https://github.com/cfobel/zmq_helpers",
      license="GPL",
      long_description="""""",
      packages=['zmq_helpers'])
