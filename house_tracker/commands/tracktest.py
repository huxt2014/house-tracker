
import os
import logging
import time
from datetime import datetime

from . import Command
from . import track_community
from house_tracker.db import get_session
from house_tracker.modules import (Community, House, CommunityRecord, 
                                   HouseRecord)
from house_tracker.utils.conf_tool import GlobalConfig
from house_tracker.utils.exceptions import ParseError, DownloadError

logger = logging.getLogger(__name__)
date_today = datetime.today().strftime('%Y-%m-%d')

class TrackTest(Command):
    def __init__(self):
        Command.__init__(self, debug=True)
        
    def run(self):
        
        session = get_session()
        try:
            community = session.query(Community).order_by(Community.id)[0]
        except IndexError:
            print 'no community found.'
        else:
            track_community(community, session)
        finally:
            session.close()

def run():
    TrackTest().run()

    



        
        