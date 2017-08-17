# coding=utf-8

import types
import unittest
import requests
from sqlalchemy import func
from sqlalchemy.orm.exc import NoResultFound

from house_tracker import config, db
from house_tracker.models import (BatchJob, DistrictFD, CommunityFD, BatchJobFD,
                                  DistrictJob, PresalePermit, base)


class Test(unittest.TestCase):

    config = None
    http_session = None
    db_session = None
    district = None
    clean_district = False

    @classmethod
    def setUpClass(cls):
        cls.config = config.Config()
        db.init(cls.config)
        cls.db_session = db.Session()

        try:
            cls.district = (cls.db_session.query(DistrictFD)
                            .filter_by(name="卢湾区")
                            .one())
        except NoResultFound:
            cls.district = DistrictFD("卢湾区", 3)
            cls.db_session.add(cls.district)
            cls.db_session.commit()
            cls.clean_district = True

    @classmethod
    def tearDownClass(cls):
        if cls.clean_district:
            cls.db_session.delete(cls.district)
            cls.db_session.commit()

    def setUp(self):
        self.http_session = requests.Session()

    def tearDown(self):
        self.db_session.rollback()

    def test_1(self):
        content = self.district.fd_search(self.http_session, 1)
        c_list = content["community_list"]
        self.assertTrue(content["total_page"] > 0)

        c_info = c_list[0]
        c = CommunityFD(c_info.pop("name"), c_info.pop("outer_id"),
                        self.district, **c_info)
        c.load_detail(self.http_session)
        c.check_presale_permit(self.db_session, self.http_session)
        self.assertTrue(len(c.presales) > 0)
        self.db_session.add(c)
        self.db_session.flush()
        self.http_session.close()

    def test_2(self):
        district_tmp = DistrictFD("test_tmp", 100)

        batch_job_1 = BatchJob.get_batch_job(BatchJobFD, self.db_session,
                                             create=True, force=True)
        batch_job_1.prepare_session(db_session=self.db_session,
                                    auto_commit=False)
        district_job_1 = DistrictJob(district_tmp, batch_job_1)
        district_job_1.prepare_session(db_session=self.db_session,
                                       auto_commit=False)
        community_1 = CommunityFD("tmp_1", "tmp_1", district_tmp)

        self.db_session.add_all([district_tmp, batch_job_1, district_job_1,
                                 community_1])
        self.db_session.flush()
        district_ids = [district_tmp.id, self.district.id]

        d_j_list = batch_job_1.get_district_and_job(district_ids)
        self.assertTrue(len(d_j_list) == 2, len(d_j_list))
        self.assertTrue(d_j_list[0][0] is self.district)
        self.assertTrue(d_j_list[0][1] is None)
        self.assertTrue(d_j_list[1][0] is district_tmp)
        self.assertTrue(d_j_list[1][1] is district_job_1,
                        "%s is not %s" % (d_j_list[1][1], district_job_1))

        self.http_session.close()

    def test_9(self):
        cmd_args = types.SimpleNamespace(force=True, create=True)
        district_ids = [self.district.id]

        def check_batch_result(batch_job):
            self.assertTrue(batch_job.status == base.FINISHED)

            district_jobs = (self.db_session.query(DistrictJob)
                             .filter_by(batch_type=batch_job.type,
                                        batch_number=batch_job.batch_number)
                             .all())
            self.assertTrue(len(district_jobs) > 0)
            for job in district_jobs:
                self.assertTrue(job.status == base.FINISHED, job.id)

        # run once
        batch_job_1 = BatchJob.run_batch(BatchJobFD, config=self.config,
                                         cmd_args=cmd_args,
                                         db_session=self.db_session,
                                         district_ids=district_ids,
                                         auto_commit=False)
        check_batch_result(batch_job_1)

        # delete one community, track only one community
        community_list = (self.db_session.query(CommunityFD)
                          .filter_by(district_id=self.district.id)
                          .all())
        community_number = len(community_list)
        (self.db_session.query(PresalePermit)
         .filter_by(community_id=community_list[0].id)
         .delete())
        self.db_session.delete(community_list[0])
        for c in community_list[1:-1]:
            c.track_presale = False
        self.db_session.flush()

        # run once again
        batch_job_2 = BatchJob.run_batch(BatchJobFD, config=self.config,
                                         cmd_args=cmd_args,
                                         db_session=self.db_session,
                                         district_ids=district_ids,
                                         auto_commit=False)
        check_batch_result(batch_job_2)
        number_2 = (self.db_session.query(func.count(CommunityFD.id))
                    .filter_by(district_id=self.district.id)
                    .scalar())
        self.assertTrue(community_number == number_2,
                        "%s, %s" % (community_number, number_2))
        self.http_session.close()
