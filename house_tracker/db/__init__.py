from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

engine = None
# a session factory, name it as class name style
Session = None


def get_database_url():
    from house_tracker.utils.conf_tool import GlobalConfig
    
    config = GlobalConfig().database
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
