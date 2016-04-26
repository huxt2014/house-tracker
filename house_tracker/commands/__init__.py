
import sys
import logging.config

from house_tracker import db
from house_tracker.modules import (House, CommunityRecord, HouseRecord, 
                                   week_number)
from house_tracker.utils import download_community_pages, download_house_page
from house_tracker.utils.conf_tool import GlobalConfig
from house_tracker.utils.exceptions import ParseError, DownloadError

logger = logging.getLogger(__name__)

class Command():
    
    def __init__(self, debug=False):
        pass
        
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
        finish_num = 0
        logger.info('download and parse house begin ...')
        for outer_id in house_ids:
            finish_num += 1
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
                    house.price_change = price_old - h_record.price
                
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
                if finish_num % 10 == 0:
                    logger.info('%s houses finish ...' % finish_num)
                logger.debug(h_record)
        # update
        session.flush()
        logger.info('all %s houses finish.' % finish_num)
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
            rs = session.execute(db.house_aggregate_community_sql,
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
        
    
    