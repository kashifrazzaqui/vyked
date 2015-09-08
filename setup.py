# -*- coding: utf-8 -*-
from setuptools import setup, find_packages
from os import getcwd, path
from pip.req import parse_requirements
from pip.download import PipSession

import codecs
import os
import re

if not path.dirname(__file__):  # setup.py without /path/to/
    _dirname = getcwd()  # /path/to/
else:
    _dirname = path.dirname(path.dirname(__file__))


def read(name, default=None, debug=True):
    try:
        filename = path.join(_dirname, name)
        with open(filename) as f:
            return f.read()
    except Exception as e:
        err = "%s: %s" % (type(e), str(e))
        if debug:
            print(err)
        return default


def lines(name):
    txt = read(name)
    return map(
        lambda l: l.lstrip().rstrip(),
        filter(lambda t: not t.startswith('#'), txt.splitlines() if txt else [])
    )

install_reqs = parse_requirements("./requirements/base.txt", session=PipSession())
install_requires = [str(ir.req) for ir in install_reqs]

with codecs.open(os.path.join(os.path.abspath(os.path.dirname(
        __file__)), 'vyked', '__init__.py'), 'r', 'latin1') as fp:
    try:
        version = re.findall(r"^__version__ = '([^']+)'\r?$",
                             fp.read(), re.M)[0]
    except IndexError:
        raise RuntimeError('Unable to determine version.')

setup(
    name='vyked',
    version=version,
    author='Kashif Razzaqui, Ankit Chandawala',
    author_email='kashif.razzaqui@gmail.com, ankitchandawala@gmail.com',
    url='https://github.com/kashifrazzaqui/vyked',
    description='A micro-service framework for Python',
    packages=find_packages(exclude=['examples', 'tests', 'docs']),
    package_data={'requirements': ['*.txt']},
    install_requires=install_requires
)
