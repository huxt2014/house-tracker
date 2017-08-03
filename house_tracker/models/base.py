# coding=utf-8

import os
import json
import logging
from datetime import datetime

import requests
from sqlalchemy import (Column, ForeignKey, types, inspect, desc,
                        PrimaryKeyConstraint, ForeignKeyConstraint)
from sqlalchemy.dialects.mysql import BINARY, VARCHAR, INTEGER, DATETIME, TEXT
from sqlalchemy.orm import relationship, backref, joinedload
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta

from .. import db
from ..exceptions import JobError, BatchJobError, ParseError, DownloadError


logger = logging.getLogger(__name__)

READY = "ready"
RUNNING = "running"
FAILED = "failed"
FINISHED = "finished"
RETRY = "retry"


class PickleType(types.PickleType):

    def __init__(self):
        types.PickleType.__init__(self, pickler=json)

    impl = TEXT


class IdMixin:
    id = Column(INTEGER, primary_key=True, autoincrement=True)


class StatusMixin:
    status = Column(VARCHAR(16))


class SessionMixin:
    db_session = None
    http_session = None
    auto_commit = None

    def prepare_session(self, db_session=None, http_session=None,
                        auto_commit=None):
        self.db_session = db_session
        self.http_session = http_session
        self.auto_commit = auto_commit

    def commit(self):
        if self.auto_commit:
            self.db_session.commit()
        else:
            self.db_session.flush()


class CommonBase:

    created_at = Column(DATETIME, default=datetime.now)
    last_modified_at = Column(DATETIME, onupdate=datetime.now)

    __table_args__ = {
        'mysql_engine': 'InnoDB',
        'mysql_charset': 'utf8',
        'mysql_collate': 'utf8_bin',
    }

    def __str__(self):

        out = ''
        mapper = inspect(self.__class__)
        for c in mapper.columns.keys():
            v = getattr(self, c)
            if isinstance(v, datetime):
                out += '%s:%s ' % (c, v.isoformat(' '))
            else:
                out += '%s:%s ' % (c, v)
        return out.strip()


Base = declarative_base(cls=CommonBase)


class District(Base, IdMixin):
    """记录区：黄埔区，浦东新区..."""
    __tablename__ = 'district'

    name = Column(VARCHAR(64), unique=True)
    outer_id = Column(INTEGER)

    def __init__(self, name, outer_id=None):
        Base.__init__(self, name=name, outer_id=outer_id)


class Area(Base, IdMixin):
    """记录板块：联洋板块、杨东板块..."""
    __tablename__ = 'area'

    name = Column(VARCHAR(64))
    district_id = Column(INTEGER, ForeignKey("district.id"))

    district = relationship('District')

    def __init__(self, name, district):
        Base.__init__(self, district=district, name=name,
                      district_id=district.id)


class Community(Base, IdMixin):
    """记录小区，包括已建成小区和在售楼盘"""
    __tablename__ = 'community'

    district_id = Column(INTEGER, ForeignKey("district.id"))
    area_id = Column(INTEGER, ForeignKey('area.id'))
    outer_id = Column(VARCHAR(128))
    name = Column(VARCHAR(64), nullable=False)
    type = Column(VARCHAR(16), nullable=False)

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


class BatchJob(Base, StatusMixin, SessionMixin):
    __tablename__ = 'batch_job'

    batch_number = Column(INTEGER, nullable=False)
    type = Column(BINARY(8), nullable=False)

    # jobs = relationship(Job)

    jobs_unfinished = relationship(
                        'Job',
                        primaryjoin="(BatchJob.batch_number==Job.batch_number)"
                                    "&(BatchJob.type==Job.batch_type)"
                                    "&(Job.status!='%s')" % FINISHED)

    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': b"batch" + bytes(3),
    }

    __table_args__ = (
        PrimaryKeyConstraint('batch_number', 'type'),
        Base.__table_args__
    )

    cache_dir = None

    def __init__(self, batch_number):
        self.batch_number = batch_number
        self.status = READY

    def start(self, db_session, http_session, auto_commit=True, cache_dir=None,
              **kwargs):
        """This method do not catch JobError, because base class doesn't know
        how to deal with the error. JobError is cached in _start method."""

        self.prepare_dir(cache_dir)
        self.prepare_session(db_session, http_session, auto_commit)

        for job in self.jobs_unfinished:
            if job.status not in (READY, RETRY):
                job.status = RETRY
        self.commit()

        self.status = self._start(**kwargs)
        self.commit()

    def prepare_dir(self, root):
        if root is None:
            raise BatchJobError("cache dir for batch job is None")
        type = self.type.decode("ascii").replace('\x00', '')
        self.cache_dir = os.path.join(root, type, str(self.batch_number))
        os.makedirs(self.cache_dir, exist_ok=True)

    @staticmethod
    def run_batch(cls, config=None, cmd_args=None, db_session=None,
                  auto_commit=True, **kwargs):

        db_session = db_session or db.Session(config)
        http_session = requests.Session()
        batch_job = BatchJob.get_batch_job(cls, db_session,
                                           create=cmd_args.create,
                                           force=cmd_args.force)

        if not batch_job:
            return
        elif batch_job.status == FINISHED:
            logger.info("the last batch job has already finished.")
            return

        try:
            logger.info("batch job start...")
            batch_job.start(db_session, http_session,
                            cache_dir=config.data_dir, auto_commit=auto_commit,
                            **kwargs)
            logger.info("batch job finished")
        except BatchJobError as e:
            # all objects' status should have been already set
            # appropriately and nothing is done here.
            logger.error(e)
        except (InterruptedError, Exception) as e:
            # Keyboard interrupt or unknown exception happened
            # log and rollback session
            logger.exception(e)
            db_session.rollback()
            logger.warning("run_batch rollback")
            batch_job.status = FAILED

        if auto_commit:
            db_session.commit()
        return batch_job

    @staticmethod
    def get_batch_job(cls, db_session, create=False, force=False):

        last_batch_job = (db_session.query(cls)
                          .options(joinedload('jobs_unfinished'))
                          .order_by(desc(cls.batch_number))
                          .first())

        if last_batch_job is None:
            if not create:
                logger.info("no unfinished batch job found.")
                return
            else:
                current_batch_job = cls(1)
                db_session.add(current_batch_job)
                db_session.flush()
        else:
            if not create:
                current_batch_job = last_batch_job
            else:
                if last_batch_job.status == FINISHED or force:
                    current_batch_job = cls(last_batch_job.batch_number+1)
                    db_session.add(current_batch_job)
                    db_session.flush()
                else:
                    logger.info('last batch not finished yet.')
                    return

        return current_batch_job

    def _get_obj_and_job(self, obj_cls, job_cls, on_foreign, filter_=None,
                         order=None):
        query = (self.db_session.query(obj_cls, job_cls)
                 .outerjoin(job_cls,
                            on_foreign
                            & (job_cls.batch_number == self.batch_number)
                            & (job_cls.batch_type == self.type)))
        if filter_ is not None:
            query = query.filter(filter_)
        if order is not None:
            query = query.order_by(order)
        return query.all()

    def _start(self, **kwargs):
        """This method should catch JobError and deal with it appropriately."""
        return FINISHED

    def __str__(self):
        return '%s-BatchJob-%s-%s' % (id(self), self.type, self.batch_number)

    __repr__ = __str__


class Job(Base, IdMixin, StatusMixin, SessionMixin):

    __tablename__ = 'job'

    batch_number = Column(INTEGER, nullable=False)
    batch_type = Column(BINARY(8), nullable=False)

    parameters = Column(PickleType)
    type = Column(VARCHAR(16))

    batch_job = relationship(BatchJob, backref=backref('jobs'))

    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'job',
    }

    __table_args__ = (
        ForeignKeyConstraint(('batch_number', 'batch_type'),
                             ('batch_job.batch_number', 'batch_job.type')),
        Base.__table_args__
    )

    def __init__(self, batch_job, parameters=None):
        Base.__init__(self, batch_job=batch_job, status=READY,
                      batch_number=batch_job.batch_number,
                      batch_type=batch_job.type)
        self.parameters = parameters or {}

    def start(self, db_session, http_session, auto_commit=True,
              **kwargs):

        self.prepare_session(db_session, http_session, auto_commit)
        if self.status not in (READY, RETRY):
            raise JobError("can not start job in status '%s', id: %s"
                           % (self.status, self.id))

        self.status = self._start(**kwargs)
        self.commit()

    def _start(self, **kwargs):
        """This method may raise JobError. Cleaning up should be done before
        JobError raised.
        """
        return FINISHED

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

    def load_from_disk(self):
        file_path = self.cache_file_path

        if not self.target_uri:
            self.target_uri = file_path

        with open(file_path, 'rb') as f:
            content = f.read().decode('utf-8')

        self.use_cached = True

        return content

    def disk_uri(self):
        return '%s-%s-%s' % (self.batch_number, self.type, self.id)

    @property
    def cache_path(self):
        return os.path.join(self.batch_job.cache_dir, self.disk_uri())


class JobWithCommunity(Job):

    __tablename__ = Job.__tablename__

    __table_args__ = {
        'extend_existing': True,
    }

    community_id = Column(INTEGER)

    def __init__(self, community, batch_job, parameters=None):
        Job.__init__(self, batch_job, parameters)
        self.community = community
        self.community_id = community.id


class PagesIterator:

    def __init__(self, job, search_func):
        self.job = job
        self.search_func = search_func
        self.total_page = job.parameters.get("total_page", None)
        self.next_page = job.parameters.get("next_page", 1)
        self.http_session = job.http_session
        self.params = {}

    def __iter__(self):
        return self

    def __next__(self):
        if self.total_page is not None and self.next_page > self.total_page:
            raise StopIteration

        try:
            content = self.search_func(self.http_session, self.next_page)
        except (ParseError, DownloadError) as e:
            # if failed, stop immediately
            raise JobError("%s: %s" % (e.__class__.__name__, e))

        if self.total_page is None:
            self.params["total_page"] = self.total_page = content["total_page"]
        if not self.params.get("total_page"):
            self.params["total_page"] = self.total_page

        self.params["next_page"] = self.next_page = self.next_page + 1
        self.job.parameters = self.params

        return content


__all__ = [key for key in list(globals().keys())
           if isinstance(globals()[key], DeclarativeMeta)]
