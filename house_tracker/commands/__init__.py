
import logging.config

from house_tracker.modules import (House, CommunityRecord, HouseRecord, 
                                   week_number)
from house_tracker.utils import download_community_pages, download_house_page
from house_tracker.utils.conf_tool import GlobalConfig
from house_tracker.utils.exceptions import ParseError, DownloadError

logger = logging.getLogger(__name__)

class Command():
    
    def __init__(self, debug=False):
        logger_config = GlobalConfig().logger_config
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
            session.commit()
    except (ParseError, DownloadError) as e:
        logger.error('parse or download community page failed: %s->%s'
                     % (community.id, community.outer_id))
        logger.exception(e)
        return False
    
    logger.debug(community)
    
    # download and parse house
    try:
        parse_error = 0
        for outer_id in house_ids:
            try:
                try:
                    house = (session.query(House)
                             .filter(House.outer_id == outer_id))[0]
                except IndexError:
                    house = House(outer_id=outer_id, 
                                  community_id=community.id)
                    session.add(house)
                    if not debug:
                        session.commit()
                    else:
                        session.flush()
                
                h_record = HouseRecord(house_id=house.id,
                                       community_id = community.id)
                download_house_page(house, h_record, community.outer_id)
                
                logger.debug(house)
            except ParseError as e:
                # download finish but parse failed.
                parse_error += 1
                logger.error('parse house page failed: %s' % outer_id)
                logger.exception(e)
                logger.debug(house)
                continue
            else:
                # download and parse success
                session.add_all([house, h_record])
                logger.debug(h_record)
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
    else:
        c_record = (session.query(CommunityRecord)
                           .filter(CommunityRecord.id == c_record.id))[0]
        c_record.house_download_finish = True
        if parse_error == 0:
            c_record.house_parse_finish = True
        session.add(c_record)
        if not debug:
            session.commit()
        
        logger.debug(c_record)
        return True
        
    
    