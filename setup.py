from distutils.core import setup

setup(name='vyked',
      version='0.1',
      author='Kashif Razzaqui',
      author_email='kashif.razzaqui@gmail.com',
      url='https://github.com/kashifrazzaqui/vyked',
      description='A micro-service framework for Python',
      packages=['vyked', 'vyked.utils'], requires=['again', 'aiohttp', 'jsonstreamer', 'setproctitle'])

