
import math
import logging
from datetime import date


from . import Command, track_community, confirm_result
from common.db import get_session
from house_tracker.models import (Community, House, CommunityRecord, 
                                  HouseRecord)
from house_tracker.utils.exceptions import ParseError, DownloadError


import settings
logger = logging.getLogger(__name__)

class Track(Command):
    def __init__(self):
        Command.__init__(self)
        
    def run(self):
        logger.info('track start...')
        force = False
        error_num = 0
        day_number = (date.today() - settings.original_date).days
        week_number = int(math.ceil(day_number / 7.0))
        
        try:
            session = get_session()
            communities = session.query(Community).order_by(Community.id)
    
            if not force:
                communities = [c for c in communities 
                               if (c.last_track_week < week_number 
                                   or c.last_track_week is None)]        
            logger.info('%s community to track' % len(communities))
        
            for community in communities:
                if not track_community(community, session):
                    error_num += 1
            
        except Exception:
            session.rollback()
            raise        
        finally:
            session.close()
        
        if  error_num:
            logger.warn('%s failed' % error_num )
        else:
            logger.warn('finish all')
        
        confirm_result()

def run():
    try:
        Track().run()
    except Exception as e:
        logger.exception(e)
        raise

    



        
        