
import os
import logging
import time
from datetime import datetime

from . import Command
from house_tracker.db import get_session
from house_tracker.modules import (Community, House, CommunityRecord, 
                                   HouseRecord)

logger = logging.getLogger(__name__)
date_today = datetime.today().strftime('%Y-%m-%d')

class TrackTest(Command):
    def __init__(self):
        Command.__init__(self, debug=True)
        
    def run(self):
        
        session = get_session()
        communities = session.query(Community).order_by(Community.id)
        
        if not communities:
            logger.info('begin...')
            logger.warn('your community table is empty! ')
        else:
            logger.info(communities[0])
            logger.warn('this is a warning!')
        

def run():
    TrackTest().run()

    



        
        