
from datetime import datetime

from sqlalchemy import create_engine, Column, Integer, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base, declared_attr 
from sqlalchemy.orm.collections import InstrumentedList


Base = declarative_base()

class BaseMixin(object):
    
    @declared_attr
    def __tablename__(cls):
        name_list = []
        for i, char in enumerate(cls.__name__):
            if i == 0 or char.islower():
                name_list.append(char.lower())
            else:
                name_list.append('_'+char.lower())
        return ''.join(name_list)
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.now)
    last_modified_at = Column(DateTime, onupdate=datetime.now)

    def __repr__(self):
        content = 'id->%s ' % self.id
        attrs = [key for key in dir(self) 
                 if not key.startswith('_') and key != 'id' 
                    and key != 'metadata']
        for attr in attrs:
            if isinstance(getattr(self, attr), InstrumentedList):
                continue
            content += '%s->%s ' % (attr, getattr(self, attr))
        return content
    


engine = None
# a session factory, name it as class name style
Session = None

def get_database_url():
    import house_tracker_settings as settings
    
    config = settings.database
    return  ('%s://%s:%s@%s/%s?charset=utf8' % 
             (config['driver'], config['user'], config['password'],
              config['host'], config['name'])
             )

def init_db():
    global engine, Session
    if not engine:
        engine = create_engine(get_database_url(), encoding='utf-8')
    if not Session:
        Session = sessionmaker(bind=engine)
    
def get_engine():
    if not engine:
        init_db()
    return engine

def get_session():
    if not Session:
        init_db()
    return Session()

