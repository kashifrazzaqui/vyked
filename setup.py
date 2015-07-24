from setuptools import setup, find_packages

setup(name='vyked',
      version='1.2.51',
      author='Kashif Razzaqui, Ankit Chandawala',
      author_email='kashif.razzaqui@gmail.com, ankitchandawala@gmail.com',
      url='https://github.com/kashifrazzaqui/vyked',
      description='A micro-service framework for Python',
      packages=find_packages(exclude=["examples"]),
      install_requires=['again', 'aiohttp', 'jsonstreamer', 'setproctitle', 'aiopg', 'asyncio_redis', 'async_retrial'])
