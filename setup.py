from setuptools import setup

setup(
   name='s3cache',
   version='0.1',
   description='A module that Caches Database queries to S3',
   author='Radu Lucaciu',
   author_email='',
   packages=['s3cache'],  #same as name
   install_requires=['pandas', 'sqlalchemy'], #external packages as dependencies
)