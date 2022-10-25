from setuptools import setup

setup(
   name='s3cache',
   version='0.2',
   description='A module that Caches Database queries to S3',
   author='Radu Lucaciu',
   author_email='',
   packages=['s3cache'],  #same as name
   install_requires=['fastparquet', 'pandas', 'sqlalchemy'], #external packages as dependencies
)