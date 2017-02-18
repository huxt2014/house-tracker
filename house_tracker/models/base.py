
import os
import json
import time
import logging
from datetime import datetime

import requests
from sqlalchemy import Column, ForeignKey, types, inspect
from sqlalchemy.dialects.mysql import VARCHAR, INTEGER, DATETIME, TEXT
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta

from ..exceptions import *


Base = declarative_base()
logger = logging.getLogger(__name__)


class PickleType(types.PickleType):

    def __init__(self):
        types.PickleType.__init__(self, pickler=json)

    impl = TEXT


class BaseMixin(object):
    id = Column(INTEGER, primary_key=True, autoincrement=True)
    created_at = Column(DATETIME, default=datetime.now)
    last_modified_at = Column(DATETIME, onupdate=datetime.now)

    def __str__(self):
        out = ''
        mapper = inspect(self.__class__)
        for c in mapper.columns.keys():
            v = getattr(self, c)
            if isinstance(v, datetime):
                out += '%s:%s ' % (c, v.isoformat(' '))
            elif type(v) in (str, unicode):
                out += '%s:%s ' % (c, v.encode('utf-8'))
            else:
                out += '%s:%s ' % (c, v)
        return out.strip()


class District(BaseMixin, Base):
    __tablename__ = 'district'

    name = Column(VARCHAR(64))


class Area(BaseMixin, Base):
    __tablename__ = 'area'

    name = Column(VARCHAR(64))
    district_id = Column(INTEGER, ForeignKey("district.id"))

    district = relationship('District')


class Community(BaseMixin, Base):
    __tablename__ = 'community'

    district_id = Column(INTEGER, ForeignKey("district.id"))
    area_id = Column(INTEGER, ForeignKey('area.id'))
    outer_id = Column(VARCHAR(128))
    name = Column(VARCHAR(64), nullable=False)
    area_name = Column(VARCHAR(64))
    type = Column(VARCHAR(64), nullable=False)

    area = relationship('Area', foreign_keys=area_id)
    district = relationship('District', foreign_keys=district_id)
    # jobs = relationship

    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'community',
    }

    def __init__(self, name, outer_id, district, area=None):
        Base.__init__(self, name=name, outer_id=outer_id, district=district,
                      district_id=district.id)
        if area:
            self.area = area
            self.area_id = area.id
            self.area_name = area.name


class BatchJob(BaseMixin, Base):
    __tablename__ = 'batch_job'

    batch_number = Column(INTEGER, nullable=False)
    status = Column(VARCHAR(64))
    type = Column(VARCHAR(64), nullable=False)

    jobs_unsuccessful = relationship('Job', foreign_keys='Job.batch_job_id',
                                     primaryjoin="(BatchJob.id==Job.batch_job_id)"
                                                 "&(Job.status!='succeed')")
    # jobs = relationship(Job)

    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'batch_job',
    }

    def __init__(self, batch_number):
        self.batch_number = batch_number
        self.status = 'ready'
        self.cache_dir = None
        self.job_args = None

    def before_act(self, **kwargs):
        self.cache_dir = os.path.join(kwargs.pop('cache_dir', '/tmp'),
                                      self.type, str(self.batch_number))
        if not os.path.isdir(self.cache_dir):
            os.makedirs(self.cache_dir)

        self.job_args = {'interval_time': kwargs.get('interval_time'),
                         'clean_cache': kwargs.get('clean_cache'),
                         }

    def initial(self, session, **kwargs):
        self.before_act(**kwargs)
        try:
            self._initial(session)
        except JobError as e:
            session.rollback()
            logger.warn('batch job rollback')
            logger.error('%s failed, got job error: %s', self, e)
            raise BatchJobError(e.__str__())
        else:
            session.commit()
            logger.info('batch job init finish: %s', self)

    def start(self, session, **kwargs):
        self.before_act(**kwargs)

        (session.query(Job)
         .filter_by(batch_job_id=self.id, status='failed')
         .update({Job.status: 'retry'}))

        try:
            self._start(session)
        except JobError as e:
            session.rollback()
            logger.warn('batch job rollback')
            logger.error('%s failed, got job error: %s', self, e)
            raise BatchJobError(e.__str__())
        else:
            session.commit()
            logger.info('batch job finish %s', self)

    def _initial(self, session):
        raise NotImplementedError

    def _start(self, session):
        raise NotImplementedError

    def __str__(self):
        return '%s-th batch job for %s' % (self.batch_number, self.type)


class Job(BaseMixin, Base):
    """
    The following method/attribute should be overrided:
        inner_start
        web_uri
    The following methd/attribute can be overrided:
        web_encode
        disk_uri (for disk cache)
    """
    __tablename__ = 'job'

    batch_job_id = Column(INTEGER, ForeignKey('batch_job.id'))

    batch_number = Column(INTEGER)
    status = Column(VARCHAR(64))
    target_uri = Column(VARCHAR(1024))
    parameters = Column(PickleType)
    type = Column(VARCHAR(64))

    batch_job = relationship(BatchJob, foreign_keys=batch_job_id,
                             backref=backref('jobs'))

    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'job',
    }

    use_cached = False
    _cache_file_path = None

    def __init__(self, batch_job, parameters):
        Base.__init__(self, batch_job=batch_job, status='ready')
        self.parameters = parameters or {}
        # a job may belongs to no batch job
        if batch_job:
            self.batch_job_id = batch_job.id
            self.batch_number = batch_job.batch_number

    def start(self, session, auto_commit=True, clean_cache=False,
              interval_time=None, **kwargs):
        if clean_cache:
            # clean cached file before job start
            path = self.cache_file_path
            if path and os.path.isfile(path):
                logger.debug('remove cached file: %s', path)
                os.remove(path)

        if self.status not in ('ready', 'retry'):
            msg = 'can not start a %s job' % self.status
            logger.error(msg)
            raise JobError(msg)

        try:
            self.inner_start(session, **kwargs)
        except (ModelError, KeyboardInterrupt) as e:
            if isinstance(e, KeyboardInterrupt):
                logger.exception(e)
            session.rollback()
            logger.warn('job rollback')
            (session.query(Job)
             .filter_by(id=self.id)
             .update({Job.status: 'failed',
                      Job.target_uri: self.web_uri()}))
            raise JobError('job failed.')
        else:
            self.status = 'succeed'
            if auto_commit:
                session.commit()
        finally:
            if not self.use_cached and interval_time:
                time.sleep(interval_time)

    def inner_start(self, session, **kwargs):
        """ Called by self.start. If any exception throw out, transaction will
        roll back, and job status is set as failed"""

        raise NotImplementedError

    def get_web_page(self, cache=False):
        """get the content of target web page. Load from disk if already cached.
        """
        # try to get cached file
        path = self.cache_file_path
        if path and os.path.isfile(path):
            logger.info('%s -> %s.', self.web_uri(), path)
            return self.load_from_disk()

            # get from web
        if cache and not path:
            logger.error('get cache file path failed.')
            raise JobError
        else:
            return self.load_from_web(cache)

    def load_from_web(self, cache):
        try_times = 0
        self.target_uri = url = self.web_uri()
        logger.debug('download web page: %s', url)

        while True:
            try:
                try_times += 1
                res = requests.get(url, timeout=3)
            except (requests.exceptions.RequestException,
                    requests.exceptions.Timeout) as e:
                if try_times < 3:
                    logger.warn(e)
                    logger.warn('%s-th try failed: %s.', try_times, url)
                    time.sleep(3)
                else:
                    logger.exception(e)
                    logger.error('get html failed: %s', url)
                    raise DownloadError
            else:
                break

        if res.status_code != 200:
            logger.error('bad response: %s, %s', res.status_code, self.url)
            raise DownloadError
        res.encoding = self.web_encode()

        if cache and self.cache_file_path:
            with open(self.cache_file_path, 'wb') as f:
                f.write(res.text.encode('utf-8'))

        return res.text

    def load_from_disk(self):
        file_path = self.cache_file_path

        if not self.target_uri:
            self.target_uri = file_path

        with open(file_path, 'rb') as f:
            content = f.read().decode('utf-8')

        self.use_cached = True

        return content

    def web_encode(self):
        return 'utf-8'

    def web_uri(self):
        raise NotImplementedError

    @property
    def cache_file_path(self):
        """return cache_dir/disk_uri. disk_uri is got by method self.disk_uri.
        """
        if self._cache_file_path is None:
            if not hasattr(self, 'disk_uri'):
                self._cache_file_path = ''
            else:
                cache_dir = (self.batch_job.cache_dir if self.batch_job
                             else '/tmp')
                self._cache_file_path = os.path.join(cache_dir, self.disk_uri())
        return self._cache_file_path


class JobWithCommunity(Job):
    community_id = Column(INTEGER, ForeignKey('community.id'))
    community = relationship('Community', backref=backref('jobs'),
                             foreign_keys=community_id)
    __mapper_args__ = {
        'polymorphic_identity': 'community_job',
        }

    def __init__(self, community, batch_job, parameters=None):
        Job.__init__(self, batch_job, parameters)
        self.community = community
        self.community_id = community.id


__all__ = [key for key in list(globals().keys())
           if isinstance(globals()[key], DeclarativeMeta)]
