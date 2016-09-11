
from setuptools import setup, find_packages
import house_tracker


setup(
    name='house_tracker',
    version=house_tracker.version,
    description='Tool to track house information in sh.lianjia.com',
    author='Terrence Hu',
    author_email='huxt2013@163.com',
    url="https://github.com/huxt2014/house-tracker",
    packages=find_packages(),
    scripts = ['house_tracker_settings.py', 'house_tracker.py'],
    include_package_data=True,
    install_requires=['alembic==0.8.7', 'requests>=2.9.1', 'SQLAlchemy>=1.0.12',
                      'MySQL-python>=1.2.5', 'flask>=0.10.1', 
                      'flask_admin>=1.4.0', 'bs4'],
)
