from setuptools import setup

setup(name='vyked',
      version='1.2.57',
      author='Kashif Razzaqui, Ankit Chandawala',
      author_email='kashif.razzaqui@gmail.com, ankitchandawala@gmail.com',
      url='https://github.com/kashifrazzaqui/vyked',
      description='A micro-service framework for Python',
      packages=['vyked', 'vyked.utils'], install_requires=['again', 'aiohttp', 'jsonstreamer', 'setproctitle', 'aiopgx',
                                                           'asyncio_redis', 'async_retrial'])
