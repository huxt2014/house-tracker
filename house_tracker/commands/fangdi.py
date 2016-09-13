# coding=utf-8

import logging
from sqlalchemy import func
from sqlalchemy.orm import joinedload

from house_tracker.models import *
from house_tracker.utils.db import get_session
from . import Command

logger = logging.getLogger(__name__)

failed_number = 0


def run():
    global failed_number
    
    Command()
    session = get_session()
    
    current_batch = session.query(func.max(DistrictJob.batch_number)).scalar()
    
    if current_batch is None:
        current_batch = 1
    
    # check job, initial job if not exist
    for district in session.query(District).all():
        job_num = (session.query(func.count('*'))
                          .select_from(DistrictJob)
                          .filter_by(batch_number=current_batch,
                                     district_id=district.id)
                          .scalar())
        if job_num:
            continue
        else:
            district.init_batch_jobs(session, current_batch)
    
    '''
    # finish all jobs
    jobs = (session.query(DistrictJob).options(joinedload('district'))
                   .filter_by(batch_number=current_batch,
                              status='ready')
                   .all())
    '''
    jobs = (session.query(PresaleJob)
                   .filter_by(batch_number=current_batch,
                              status='ready')
                   .all())
    
    for job in jobs:
        try:
            job.start(session)
        except Exception as e:
            print e
            failed_number += 1
    
    
    print 'error number: %s' % failed_number
    







    





