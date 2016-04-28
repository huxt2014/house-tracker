# coding=utf-8

import os
import logging
import time
import tempfile
from datetime import datetime

from . import Command, track_community
from common.db import get_session
from house_tracker.models import (Community, House, CommunityRecord, 
                                  HouseRecord, week_number)
from house_tracker.utils.exceptions import ParseError, DownloadError


import settings
logger = logging.getLogger(__name__)
date_today = datetime.today().strftime('%Y-%m-%d')

class TrackTest(Command):
    def __init__(self):
        Command.__init__(self, debug=True)
        settings.data_dir = tempfile.mkdtemp()
        
    def run(self):
        logger.info('tracktest start...')
        session = get_session()
        try:
            session.add(Community(outer_id='5011000018309', name=u'万邦都市花园',
                                  district=u'浦东新区'))
            session.add(Community(outer_id='5011000012349', name=u'浦东星河湾',
                                  district=u'浦东新区'))
                
            communities = (session.query(Community)
                           .filter(Community.outer_id.in_(('5011000018309',
                                                           '5011000012349'))
                                   )
                           )
        
            for community in communities:
                track_community(community, session, debug=True)
                c_record = (session.query(CommunityRecord)
                            .filter_by(community_id=community.id,
                                       create_week=week_number())
                            [0]
                            )
                h_records = (session.query(HouseRecord)
                             .filter_by(community_id=community.id,
                                        create_week=week_number())
                             .all()
                             )
                print community.__str__().encode('utf-8')
                print c_record
                print 'house available -> %s' % len(h_records)
                for record in h_records:
                    print record
                print ('*'*20 +'\n')*3
        finally:
            session.rollback()
            session.close()
        logger.info('tracktest finish')

def run():
    TrackTest().run()

    



        
        