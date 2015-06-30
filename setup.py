from distutils.core import setup

setup(name='vyked',
      version='1.2.48',
      author='Kashif Razzaqui, Ankit Chandawala',
      author_email='kashif.razzaqui@gmail.com, ankitchandawala@gmail.com',
      url='https://github.com/kashifrazzaqui/vyked',
      description='A micro-service framework for Python',
      packages=['vyked', 'vyked.utils'], install_requires=['again', 'aiohttp', 'jsonstreamer', 'setproctitle', 'aiopg',
                                                           'asyncio_redis', 'async_retrial'])
