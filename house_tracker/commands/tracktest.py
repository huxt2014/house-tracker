# coding=utf-8

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
            session.add(Community(outer_id='5011000018309', name=u'万邦都市花园',
                                  district=u'浦东新区'))
            session.add(Community(outer_id='5011000012349', name=u'浦东星河湾',
                                  district=u'浦东新区'))
                
            communities = session.query(Community).order_by(Community.id)
        
            for community in communities:
                track_community(community, session, debug=True)
                c_record = (session.query(CommunityRecord)
                            .filter_by(community_id=community.id)[0])
                h_records = (session.query(HouseRecord)
                             .filter_by(community_id=community.id)).all()
                print community.__str__().encode('utf-8')
                print c_record
                print 'house available -> %s' % len(h_records)
                for record in h_records:
                    print record
                print ('*'*20 +'\n')*3
        finally:
            session.rollback()
            session.close()

def run():
    TrackTest().run()

    



        
        