
import os
import math
import logging
import requests

from sqlalchemy import func

from . import Command, confirm_result
from house_tracker.models import *
from house_tracker.exceptions import JobError, BatchJobError
from house_tracker.utils.db import get_session


import house_tracker_settings as settings
logger = logging.getLogger(__name__)

class BatchJob(Command):
    def __init__(self):
        Command.__init__(self)
        self.interval_time = settings.interval_time
        self.session = get_session()
        
        last_batch = (self.session
                          .query(func.max(CommunityLJ.last_batch_number))
                          .scalar() )
        last_batch = last_batch or 0
        
        if self.args.new_batch:
            num_not_finish = (
                    self.session
                        .query(func.count(CommunityJobLJ.id))
                        .filter_by(batch_number=last_batch)
                        .filter(Job.status != 'succeed')
                        .scalar() )
            num_not_finish += (
                    self.session
                        .query(func.count(HouseJobLJ.id))
                        .filter_by(batch_number=last_batch)
                        .filter(Job.status != 'succeed')
                        .scalar() )
            if num_not_finish:
                raise BatchJobError('old batch not finish yet.')
            else:
                self.current_batch = last_batch + 1
        else:
            if last_batch == 0:
                raise BatchJobError('No job exists, initial a new batch.')
            else:
                self.current_batch = last_batch
                
        cache_dir = os.path.join(settings.data_dir, 
                                 'cache/%s' % self.current_batch)
        if not os.path.isdir(cache_dir):
            os.makedirs(cache_dir)
        os.environ['HOUSE_TRACKER_CACHE_DIR'] = cache_dir
        
        
    def run(self):
        logger.info('%s-th batch start...', self.current_batch)
        
        communities = self.session.query(CommunityLJ).all()
        for c in communities:
            if c.last_batch_number != self.current_batch:
                self.init_batch_jobs(c)
        
        for c in communities:
            self.finish_current_batch_jobs(c)
            
        #confirm_result()
        
    def finish_current_batch_jobs(self, community):
        logger.info('begin community jobs: %s -> %s',
                    community.id, community.outer_id)
        session = get_session()
        (session.query(CommunityJobLJ)
                .filter_by(community_id=community.id,
                           batch_number=self.current_batch,
                           status='failed')
                .update({CommunityJobLJ.status: 'retry'}) )
        (session.query(HouseJobLJ)
                .filter_by(community_id=community.id,
                           batch_number=self.current_batch,
                           status='failed')
                .update({HouseJobLJ.status: 'retry'}) )
        
        c_jobs = (session.query(CommunityJobLJ)
                         .filter_by(community_id=community.id,
                                    batch_number=self.current_batch)
                         .filter(CommunityJobLJ.status.in_(['ready', 'retry']))
                         .all() )
        for job in c_jobs:
            job.start(session, interval_time=self.interval_time)
            
        
        h_jobs = (session.query(HouseJobLJ)
                         .filter_by(community_id=community.id,
                                    batch_number=self.current_batch)
                         .filter(HouseJobLJ.status.in_(['ready', 'retry']))
                         .all() )
        for job in h_jobs:
            job.start(session, interval_time=self.interval_time)
        
        # update the state of missing houses
        (session.query(HouseLJ)
                .filter_by(last_batch_number=self.current_batch-1,
                           community_id=community.id)
                .update(
            {HouseLJ.new: False,
             HouseLJ.available: False,
             HouseLJ.available_change_times: HouseLJ.available_change_times+1})
         )
        
        # simple aggregation
        sql ="""
        select sum(case when T2.price_change> 0 then 1 else 0 end), 
               sum(case when T2.price_change< 0 then 1 else 0 end), 
               sum(case when T2.price_change = 0 
                             and (T2.view_last_month > 0 
                                  or T2.view_last_week > 0) then 1 
                        else 0 end),
               sum(T1.new),
               sum(case when T1.last_batch_number = :current_batch -1 then 1 
                        else 0 end),
               sum(case when T1.available is true then T2.view_last_week 
                        else 0 end)
        from house as T1
        left join house_record as T2
          on T1.id = T2.house_id
          and T2.batch_number = :current_batch
        where T1.community_id = :community_id""" 
        rs = session.execute(sql, {'current_batch': self.current_batch,
                                   'community_id': community.id}
                            ).fetchall()[0]
        c_record = (session.query(CommunityRecordLJ)
                           .filter_by(batch_number=self.current_batch,
                                      community_id=community.id)
                           .one() )
        (c_record.rise_number, c_record.reduce_number, 
         c_record.valid_unchange_number, c_record.new_number, 
         c_record.miss_number, c_record.view_last_week) = rs
        
        session.commit()
    
    def init_batch_jobs(self, community):
        logger.info('initial community batch jobs: %s -> %s', 
                    community.id, community.outer_id)
        try:
            job = CommunityJobLJ(community, 1, self.current_batch)
            first_page = job.get_web_page(cache=True)
            c_info, house_ids = community.parse_page(first_page, 1)
            total_page = int(math.ceil(c_info['house_available']/20.0))
            
            self.session.add(job)
            for i in range(total_page-1):
                self.session.add(CommunityJobLJ(community, i+2, self.current_batch))
            
            community.last_batch_number = self.current_batch
        except Exception:
            self.session.rollback()
            raise
        else:
            self.session.commit()

def run():
    try:
        BatchJob().run()
    except Exception as e:
        logger.exception(e)
        raise

    



        
        