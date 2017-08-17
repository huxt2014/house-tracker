# coding=utf-8

import types
import unittest
import requests
from sqlalchemy import func
from sqlalchemy.orm.exc import NoResultFound

from house_tracker import config, db
from house_tracker.models import (BatchJob, BatchJobLJ, District, Area,
                                  CommunityLJ, CommunityRecordLJ, HouseLJ,
                                  HouseRecordLJ, base)


class Test(unittest.TestCase):

    config = None
    http_session = None
    db_session = None
    district = None
    area = None

    @classmethod
    def setUpClass(cls):
        cls.config = config.Config()
        db.init(cls.config)
        cls.db_session = db.Session()

        try:
            cls.district = (cls.db_session.query(District)
                            .filter_by(name="浦东新区")
                            .one())
        except NoResultFound:
            cls.district = District("浦东新区", 14)
            cls.db_session.add(cls.district)

        try:
            cls.area = (cls.db_session.query(Area)
                        .filter_by(name="杨东板块")
                        .one())
        except NoResultFound:
            cls.area = Area("杨东板块", cls.district)
            cls.db_session.add(cls.area)

        try:
            cls.community = (cls.db_session.query(CommunityLJ)
                             .filter_by(outer_id="5011000018191")
                             .one())
        except NoResultFound:
            cls.community = CommunityLJ("上海绿城", "5011000018191", cls.area)
            cls.db_session.add(cls.community)

        cls.db_session.commit()

    def setUp(self):
        self.http_session = requests.Session()

    def tearDown(self):
        self.db_session.rollback()
        self.http_session.close()

    def test_1(self):
        info = self.community.lj_search(self.http_session, 1,
                                        self.config.lj_number_per_page)
        self.assertTrue(info["community_info"] is not None)
        self.community.update(**info["community_info"])

        h_info = info["houses_info"][0]
        house = HouseLJ(h_info.pop("outer_id"), self.community, **h_info)

        self.db_session.add_all([self.community, house])
        self.db_session.flush()

        batch_job = BatchJob.get_batch_job(BatchJobLJ, self.db_session,
                                           create=True, force=True)
        c_record = CommunityRecordLJ(self.community, batch_job,
                                     **info["community_info"])
        h_record = HouseRecordLJ(house, batch_job, price=h_info["price"])
        h_record.load_detail(self.http_session)

        self.db_session.add_all([batch_job, c_record, h_record])
        self.db_session.flush()

    def test_9(self):
        cmd_args = types.SimpleNamespace(force=False, create=False)
        community_outer_ids = [self.community.outer_id]

        # run once
        batch_job = BatchJob.run_batch(BatchJobLJ, config=self.config,
                                       cmd_args=cmd_args,
                                       db_session=self.db_session,
                                       community_outer_ids=community_outer_ids,
                                       lj_number_per_page=self.config.lj_number_per_page)
        if batch_job is None:
            cmd_args = types.SimpleNamespace(force=False, create=True)
            batch_job = BatchJob.run_batch(BatchJobLJ, config=self.config,
                                           cmd_args=cmd_args,
                                           db_session=self.db_session,
                                           community_outer_ids=community_outer_ids,
                                           lj_number_per_page=self.config.lj_number_per_page)

        self.assertTrue(batch_job.status == base.FINISHED)


"""
session.add(Community(outer_id='5011000018309', name=u'万邦都市花园',
                      district=u'浦东新区'))
session.add(Community(outer_id='5011000012349', name=u'浦东星河湾',
                      district=u'浦东新区'))"""