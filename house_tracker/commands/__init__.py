
import sys
import logging.config
from datetime import datetime, timedelta

from sqlalchemy import or_, and_
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import case

from house_tracker.utils import db
from house_tracker.utils import (house_aggregate_community_sql,
                                 download_community_pages, download_house_page)
from house_tracker.utils.exceptions import (ParseError, DownloadError, 
                                            ConfigError)
from house_tracker.models import (House, CommunityRecord, HouseRecord, 
                                  week_number)



import house_tracker_settings as settings
logger = logging.getLogger(__name__)

class Command():
    
    def __init__(self, debug=False):
        check_config()
        logger_config = settings.logger_config
        logger_config['root']['level'] = 'DEBUG' if debug else 'INFO'
        logging.config.dictConfig(logger_config)
        
    def run(self):
        raise Exception('should be override by subclass')


def track_community(community, session, debug=False):
    
    # download and parse community. If any exception, catch and return.
    try:
        try:
            c_record = (session.query(CommunityRecord)
                        .filter_by(community_id=community.id,
                                   create_week=week_number())[0]
                        )
        except IndexError:
            c_record = CommunityRecord(community_id = community.id)
        house_ids = download_community_pages(community, c_record)
        session.add_all([community, c_record])
        if not debug:
            session.flush()
            session.commit()
    except (ParseError, DownloadError) as e:
        logger.error('parse or download community page failed: %s->%s'
                     % (community.id, community.outer_id))
        logger.exception(e)
        return False
    
    logger.debug(community)
    
    # download and parse houses of the community. If any exception, all changes
    # will roll back.
    try:
        parse_error = 0
        logger.info('download and parse house begin ...')
        for index, outer_id in enumerate(house_ids):
            try:
                try:
                    house = (session.query(House)
                             .filter(House.outer_id == outer_id))[0]
                    house.new = False
                    house.last_track_week = week_number()
                    if not house.available:
                        house.available = True
                        house.available_change_times += 1
                except IndexError:
                    house = House(outer_id=outer_id, 
                                  community_id=community.id)
                    session.add(house)
                    session.flush()
                
                h_record = HouseRecord(house_id=house.id,
                                       community_id = community.id)
                price_old = house.price
                download_house_page(house, h_record, community.outer_id)
                
                if not house.new:
                    h_record.price_change = h_record.price - price_old
                else:
                    house.price_origin = house.price
                
                logger.debug(house)
            except ParseError as e:
                # download finish but parse failed, continue download and parse
                # later.
                parse_error += 1
                logger.error('parse house page failed: %s' % outer_id)
                logger.exception(e)
                logger.debug(house)
                continue
            else:
                # download and parse success
                session.add_all([house, h_record])
                if (index+1) % 10 == 0:
                    logger.info('%s houses finish ...' % (index+1))
                logger.debug(h_record)
        # update
        session.flush()
        logger.info('all %s houses finish.' % (index+1))
        (session.query(House).filter_by(last_track_week=week_number()-1,
                                        community_id=community.id)
         .update({House.new: False,
                  House.available: False,
                  House.available_change_times: House.available_change_times+1})
         )
        
    except DownloadError as e:
        # If any house page download failed, stop and return False.
        logger.error('download house page error: %s' % house.outer_id)
        logger.exception(e)
        session.rollback()
        
        logger.debug(c_record)
        logger.debug(house)
        return False
    except Exception:
        logger.error('download house page error: %s' % house.outer_id)
        session.rollback()
        raise
    except KeyboardInterrupt:
        session.rollback()
        logger.warn('exit by keyboard interrupt.')
        sys.exit(1)
    else:
        session.flush()
        c_record.house_download_finish = True
        if parse_error == 0:
            c_record.house_parse_finish = True
            rs = session.execute(house_aggregate_community_sql,
                                 {'last_track_week': week_number(),
                                  'community_id': community.id}).fetchall()[0]
            (c_record.rise_number, c_record.reduce_number, 
             c_record.valid_unchange_number, c_record.new_number, 
             c_record.miss_number, c_record.view_last_week) = rs
            
        session.add(c_record)
        if not debug:
            session.commit()
        
        logger.debug(c_record)
        return True

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
    for name in ('log_dir', 'data_dir', 'time_interval', 'logger_config',
                'original_date', 'database'):
        if not hasattr(settings, name):
            raise ConfigError('%s missing in setting file.' % name)
    
    for name in ('driver', 'host', 'name', 'user', 'password'):
        if not name in settings.database.keys():
            raise ConfigError('database server configure error: %s missing' % 
                              name)

    