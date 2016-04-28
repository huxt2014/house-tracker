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

house_aggregate_community_sql ="""
select sum(case when price_change> 0 then 1 else 0 end), 
       sum(case when price_change< 0 then 1 else 0 end), 
       sum(case when price_change = 0 
                and (view_last_month > 0 or view_last_week > 0) 
                and new is false then 1 else 0 end),
       sum(new),
       sum(case when last_track_week = :last_track_week -1 then 1 else 0 end),
       sum(case when available is true then view_last_week else 0 end)
from house
where community_id = :community_id
""" 


