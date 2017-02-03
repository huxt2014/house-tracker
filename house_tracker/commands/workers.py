
import logging
from threading import Thread

from sqlalchemy import desc
from sqlalchemy.orm import joinedload

logger = logging.getLogger(__name__)


class BatchWorker(Thread):
    def __init__(self, batch_job_cls, session, batch_job_args=None,
                 thread_args=None):
        Thread.__init__(self, **(thread_args or {}))
        self.batch_job_cls = batch_job_cls
        self.session = session
        self.batch_job_args = batch_job_args
        self.last_batch_job = None

    def run(self):
        try:
            self.last_batch_job = (
                self.session.query(self.batch_job_cls)
                .options(joinedload('jobs_unsuccessful'))
                .order_by(desc(self.batch_job_cls.batch_number))
                .first())
            self.inner_run()
        except Exception as e:
            logger.error('worker failed.')
            logger.exception(e)
            self.session.rollback()
        else:
            logger.info('worker finish')
            self.session.commit()

    def inner_run(self):
        raise NotImplementedError
    

class InitWorker(BatchWorker):
    def inner_run(self):
        if self.last_batch_job:
            if self.last_batch_job.jobs_unsuccessful:
                raise Exception('last batch job not finished.')
            else:
                current_batch_number = self.last_batch_job.batch_number+1
        else:
            current_batch_number = 1
        
        batch_job = self.batch_job_cls(current_batch_number)
        self.session.add(batch_job)
        batch_job.initial(self.session, **self.batch_job_args)
        

class StartWorker(BatchWorker):
    
    def inner_run(self):
        if not self.last_batch_job:
            logger.warn('no batch job found')
            return
        else:
            self.last_batch_job.start(self.session, **self.batch_job_args)
            self.last_batch_job.status = 'succeed'
