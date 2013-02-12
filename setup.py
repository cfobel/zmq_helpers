#!/usr/bin/env python

from distutils.core import setup

setup(name = "zmq_helpers",
    version = "0.0.1",
    description = "zmq helper functions and classes",
    keywords = "zmq helper",
    author = "Christian Fobel",
    url = "https://github.com/cfobel/zmq_helpers",
    license = "GPL",
    long_description = """""",
    packages = ['zmq_helpers'],
    package_data={'zmq_helpers': ['zmq_include/*'],
                  'zmq_helpers.shared_storage': ['zmq_templates/*',
                          'pycuda_include/*']}
)