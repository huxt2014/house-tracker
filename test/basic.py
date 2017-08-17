
import types
import shutil
import unittest

from house_tracker import db, config, models


class Test(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.config = config.Config()
        db.init(cls.config)
        cls.db_session = db.Session()
        cls.root = "/tmp/house_tracker_test"
        try:
            shutil.rmtree(cls.root)
        except FileNotFoundError:
            pass

    def tearDown(self):
        self.db_session.rollback()

    def test_1(self):
        self.db_session.execute("select 1")

        for m in (models.BatchJob, models.District):
            self.db_session.query(m).first()

    def test_2(self):
        get_batch_job = models.BatchJob.get_batch_job
        self.db_session.query(models.Job).delete()
        self.db_session.query(models.BatchJob).delete()

        # get without create
        batch_job_1 = get_batch_job(models.BatchJobFD, self.db_session)
        self.assertTrue(batch_job_1 is None)

        # no batch job exists and get with create
        batch_job_1 = get_batch_job(models.BatchJobFD, self.db_session,
                                    create=True)
        self.assertTrue(batch_job_1 is not None)
        self.assertTrue(batch_job_1.batch_number == 1)
        batch_job_1.prepare_dir(self.root)
        batch_job_1.prepare_dir(self.root)   # do again and no error

        # get without create will get the last one
        batch_job_2 = get_batch_job(models.BatchJobFD, self.db_session)
        self.assertTrue(batch_job_1 is batch_job_2)

        # faild if last batch job has not finished
        batch_job_2 = get_batch_job(models.BatchJobFD, self.db_session,
                                    create=True)
        self.assertTrue(batch_job_2 is None)

        # ok if last batch job has finished
        batch_job_1.status = models.base.FINISHED
        self.db_session.flush()
        batch_job_2 = get_batch_job(models.BatchJobFD, self.db_session,
                                    create=True)
        self.assertTrue(batch_job_1 is not batch_job_2)
        self.assertTrue(batch_job_2.batch_number == 2)

        batch_job_3 = get_batch_job(models.BatchJobFD, self.db_session,
                                    create=True)
        self.assertTrue(batch_job_3 is None)

        # create with force
        batch_job_3 = get_batch_job(models.BatchJobFD, self.db_session,
                                    create=True, force=True)
        self.assertTrue(batch_job_3 is not None)
        self.assertTrue(batch_job_3.batch_number == 3)

    def test_3(self):
        total_page = 5
        search_func = lambda http_session, next_page: {"total_page": total_page}
        job_1 = types.SimpleNamespace(parameters={}, http_session=None)
        job_2 = types.SimpleNamespace(parameters={"next_page": 3,
                                                  "total_page": total_page},
                                      http_session=None)

        num = 0
        for _ in models.PagesIterator(job_1, search_func):
            num += 1
        self.assertTrue(num == total_page, num)
        self.assertTrue(job_1.parameters.get("total_page") == total_page,
                        job_1.parameters.get("total_page", "no data"))
        self.assertTrue(job_1.parameters.get("next_page") == total_page+1,
                        job_1.parameters.get("next_page", "no data"))

        num = 0
        for _ in models.PagesIterator(job_2, search_func):
            num += 1
        self.assertTrue(num == 3, num)
        self.assertTrue(job_2.parameters.get("total_page") == total_page,
                        job_2.parameters.get("total_page", "no data"))
        self.assertTrue(job_2.parameters.get("next_page") == total_page+1,
                        job_2.parameters.get("next_page", "no data"))
