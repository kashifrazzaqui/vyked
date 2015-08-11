# -*- coding: utf-8 -*-
from setuptools import setup, find_packages
from os import getcwd, path

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


install_requires = [i for i in lines("requirements/base.txt")]

setup(
    name='vyked',
    version='2.1.5',
    author='Kashif Razzaqui, Ankit Chandawala',
    author_email='kashif.razzaqui@gmail.com, ankitchandawala@gmail.com',
    url='https://github.com/kashifrazzaqui/vyked',
    description='A micro-service framework for Python',
    packages=find_packages(exclude=['examples', 'tests', 'docs']),
    package_data={'requirements': ['*.txt']},
    install_requires=install_requires
)
