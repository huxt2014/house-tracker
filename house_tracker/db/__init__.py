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
select sum(case when T2.price_change> 0 then 1 else 0 end), 
       sum(case when T2.price_change< 0 then 1 else 0 end), 
       sum(case when T2.price_change = 0 
                and (T2.view_last_month > 0 or T2.view_last_week > 0) then 1 
                else 0 end),
       sum(T1.new),
       sum(case when T1.last_track_week = :last_track_week -1 then 1 else 0 end),
       sum(case when T1.available is true then T2.view_last_week else 0 end)
from house as T1
left join house_record as T2
  on T1.id = T2.house_id
  and T2.create_week = :last_track_week
where T1.community_id = :community_id
""" 


