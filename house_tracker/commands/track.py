
import math
import logging
from datetime import date

from . import Command
from . import track_community, confirm_result
from house_tracker.db import get_session
from house_tracker.modules import (Community, House, CommunityRecord, 
                                   HouseRecord)
from house_tracker.utils.conf_tool import GlobalConfig
from house_tracker.utils.exceptions import ParseError, DownloadError

logger = logging.getLogger(__name__)

class Track(Command):
    def __init__(self):
        Command.__init__(self)
        
    def run(self):
        force = False
        error_num = 0
        day_number = (date.today() - GlobalConfig().original_date).days
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

    



        
        