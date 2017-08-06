
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
    scripts=['house-tracker'],
    include_package_data=True,
    install_requires=['alembic==0.9.2', 'requests', 'SQLAlchemy', 'PyMySQL',
                      'flask', 'flask_admin', 'bs4==0.0.1', "blinker",
                      "pandas"],
)
