
import argparse
import logging.config
from datetime import datetime, timedelta

from sqlalchemy import or_, and_
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import case

from house_tracker.exceptions import ConfigError
from house_tracker.utils import db




import house_tracker_settings as settings
logger = logging.getLogger(__name__)

class Command():
    
    def __init__(self, debug=False):
        check_config()
        logging.config.dictConfig(settings.logger_config)
        
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument('-d', '--debug', action='store_true')
        self.parser.add_argument('--new-batch', action='store_true')
        self.parser.add_argument('--clean-cache', action='store_true')
        
        if hasattr(self, 'add_parse_arg'):
            self.add_parse_arg()
            
        self.args = self.parser.parse_args()
        if self.args.debug:
            logging.getLogger().setLevel(logging.DEBUG)
        
    def run(self):
        raise NotImplementedError



def confirm_result():
    session = db.get_session()
    # check price change
    sql = """select sum(case when T1.price > T2.price then 1 else 0 end) as rise_number,
                    sum(case when T1.price < T2.price then 1 else 0 end) as reduce_number,
                    sum(case when T1.price = T2.price
                                  and (T1.view_last_week > 0 or T1.view_last_month > 0)
                             then 1 else 0 end) as valid_unchange_number,
                    count(*) as house_available
             from house_record as T1 
             left join house_record as T2
               on T1.house_id = T2.house_id
               and T2.create_week = :create_week -1
             where T1.create_week = :create_week
         """
    h_record_join_aggr = session.execute(sql, {'create_week': week_number()}
                                         ).first()
    
    h_record_aggr = (session
                     .query(
                        func.count(HouseRecord.id).label('house_available'),
                        func.sum(case([(HouseRecord.price_change>0, 1)],
                                      else_=0)).label('rise_number'),
                        func.sum(case([(HouseRecord.price_change<0, 1)],
                                      else_=0)).label('reduce_number'),
                        func.sum(case([(and_(HouseRecord.price_change==0,
                                             or_(HouseRecord.view_last_week>0,
                                                 HouseRecord.view_last_month>0)),
                                         1)],
                                      else_=0)).label('valid_unchange_number'),
                        func.sum(HouseRecord.view_last_week).label('view_last_week')
                        )
                     .filter_by(create_week=week_number())
                     .one()
                    )
    
    for key in ('rise_number', 'reduce_number', 'valid_unchange_number'):
        try:
            assert int(getattr(h_record_join_aggr, key)) == int(getattr(h_record_aggr, key))
        except AssertionError:
            logger.error('%s in HouseRecord wrong.' % key) 
    logger.info('confirm rise_number, reduce_number, valid_unchange_number in '
                'HouseRecord finish.')
        
    # check house.available, house,new
    yesterday = (datetime.now() - timedelta(3)).strftime('%Y-%m-%d %H:%M:%S')
    house_aggr = (session
                  .query(func.sum(case([(House.created_at > yesterday, 1)],
                                       else_=0)
                                  ).label('create_number'),
                         func.sum(case([(House.available, 1)],
                                       else_=0)
                                  ).label('house_available'),
                         func.sum(case([(House.new, 1)],
                                       else_=0)).label('new_number'),
                         func.sum(case([(House.last_track_week==week_number()-1,
                                         1)],
                                       else_=0)).label('miss_number')
                         )
                  .one()
                  )
    
    try:
        assert int(house_aggr.create_number) == int(house_aggr.new_number)
    except AssertionError:
        logger.error('new_number in House wrong.')
    try:
        assert int(h_record_join_aggr.house_available) == int(house_aggr.house_available)
    except AssertionError:
        logger.error('house_available in House wrong.')
    logger.info('confirm new_number, house_available in House finishi.')
    
    # check community record
    c_record_aggr = (session
                     .query(func.sum(CommunityRecord.house_available).label('house_available'),
                            func.sum(CommunityRecord.rise_number).label('rise_number'),
                            func.sum(CommunityRecord.reduce_number).label('reduce_number'),
                            func.sum(CommunityRecord.valid_unchange_number).label('valid_unchange_number'),
                            func.sum(CommunityRecord.new_number).label('new_number'),
                            func.sum(CommunityRecord.miss_number).label('miss_number'),
                            func.sum(CommunityRecord.view_last_week).label('view_last_week'),
                            )
                     .filter_by(create_week=week_number())
                     .one()
                     )
    
    for key in ('house_available', 'rise_number', 'reduce_number', 
                'valid_unchange_number', 'view_last_week'):
        try:
            assert int(getattr(c_record_aggr, key)) == int(getattr(h_record_aggr, key))
        except AssertionError:
            logger.error('%s in CommunityRecord wrong' % key)
            
    for key in ('new_number', 'miss_number'):
        try:
            assert int(getattr(c_record_aggr, key) == getattr(house_aggr, key))
        except AssertionError:
            logger.error('%s in CommunityRecord wrong' % key)       
    logger.info('confirm CommunityRecord finish.')
        

def check_config():
    for name in ('log_dir', 'data_dir', 'logger_config', 'database'):
        if not hasattr(settings, name):
            raise ConfigError('%s missing in setting file.' % name)
    
    for name in ('driver', 'host', 'name', 'user', 'password'):
        if not name in settings.database.keys():
            raise ConfigError('database server configure error: %s missing' % 
                              name)

    