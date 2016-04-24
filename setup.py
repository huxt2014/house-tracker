
from distutils.core import setup


setup(
    name='house_tracker',
    version='0.1.0',
    description='Tool to track house information in sh.lianjia.com',
    author='Terrence Hu',
    author_email='huxt2013@163.com',
    url="https://github.com/huxt2014/house-tracker",
    packages=["house_tracker", "house_tracker.commands", "house_tracker.db", 
              "house_tracker.utils", "test", "test.test_utils"],
    install_requires=['alembic==0.8.5', 'requests==2.9.1', 'SQLAlchemy==1.0.12',
                      'MySQL-python==1.2.5'],
    data_files=[('', ['settings.py', 'manager.py', 'alembic.ini']), 
                ('migrations', ['migrations/env.py']),
                ('migrations/versions', ['migrations/versions/f966a450a2e4_.py'])]
)
