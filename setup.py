
from setuptools import setup, find_packages


setup(
    name='house_tracker',
    version='0.3.0',
    description='Tool to track house information in sh.lianjia.com',
    author='Terrence Hu',
    author_email='huxt2013@163.com',
    url="https://github.com/huxt2014/house-tracker",
    packages=find_packages(),
    py_modules = ['settings', 'manager'],
    include_package_data=True,
    install_requires=['alembic>=0.8.5', 'requests>=2.9.1', 'SQLAlchemy>=1.0.12',
                      'MySQL-python>=1.2.5', 'flask>=0.10.1', 
                      'flask_admin>=1.4.0'],
)
