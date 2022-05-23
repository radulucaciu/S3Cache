from setuptools import setup

setup(
   name='redshiftcache',
   version='0.1',
   description='A module that Caches Database queries to S3',
   author='Radu Lucaciu',
   author_email='',
   packages=['redshiftcache'],  #same as name
   install_requires=['wheel', 'bar', 'greek'], #external packages as dependencies
)