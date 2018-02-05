# coding=utf-8

import re
import math
import json
import logging
import functools

from bs4 import BeautifulSoup
from requests import Request
from sqlalchemy import (Column, ForeignKey, ForeignKeyConstraint, func, select,
                        case, text)
from sqlalchemy.dialects.mysql import BINARY, VARCHAR, INTEGER, BOOLEAN, FLOAT
from sqlalchemy.orm import relationship, backref

from . import base
from .base import (IdMixin, Base, Community, BatchJob, JobWithCommunity,
                   PagesIterator)
from ..exceptions import ParseError, JobError
from .. import utils


logger = logging.getLogger(__name__)


class CommunityLJ(Community):

    average_price = Column(INTEGER)
    house_available = Column(INTEGER)
    sold_last_season = Column(INTEGER)
    view_last_month = Column(INTEGER)
    # houses=relationship
    # community_records=relationship
    # house_records = relationship

    __mapper_args__ = {
        'polymorphic_identity': 'lianjia',
    }

    SEARCH_URL = 'http://sh.lianjia.com/ershoufang/pg%scol1c%s/'
    NUMBER_PER_PAGE = 30

    p_build_year = re.compile("(\d+)年|塔楼|板楼")
    p_area = re.compile("(\d+(\.\d+)?)平")

    def __init__(self, name, outer_id, area):
        Community.__init__(self, name, outer_id, area.district, area=area)

    def __str__(self):
        return "%s %s %s" % (self.id, self.outer_id, self.name)

    def lj_search(self, http_session, page, number_per_page=None):
        req = self._lj_search_request(page)
        resp = utils.do_http_request(http_session, req)
        return self._lj_parse(resp, page, number_per_page)

    def _lj_search_request(self, page):
        url = self.SEARCH_URL % (page, self.outer_id)
        return Request(url=url, method="GET")

    def update(self, average_price=None, house_available=None,
               sold_last_season=None, view_last_month=None):
        self.average_price = average_price
        self.house_available = house_available
        self.sold_last_season = sold_last_season
        self.view_last_month = view_last_month

    @classmethod
    def _lj_parse(cls, resp, page, number_per_page):

        soup = BeautifulSoup(resp.text, "html.parser")

        try:
            # get total page
            total_num = int(soup.find("h2", class_="total fl").find("span")
                            .get_text())
            total_page = math.ceil(total_num / number_per_page)
        except (ValueError, AttributeError) as e:
            raise ParseError("parse total number of community page failed: %s,"
                             " %s." % (resp.url, e))

        # check page index
        if total_num == 0:
            # no house_found
            on_page = 0
        else:
            raw_div = soup.find("div", class_="page-box house-lst-page-box")

            if raw_div is None:
                raise ParseError("can not find div of page information: %s"
                                 % resp.url)

            try:
                page_info = json.loads(raw_div["page-data"])
                on_page = page_info["curPage"]
            except (KeyError, json.decoder.JSONDecodeError):
                raise ParseError("parse page information failed: %s" % resp.url)

            if page != on_page:
                raise ParseError("request lianjia.com community of page %s, "
                                 "but get page %s: %s" % (
                                 page, on_page, resp.url))
        # get community info
        # record nothing, changed since version 1.0.1
        c_info = {} if on_page <= 1 else None

        # get house info
        houses_info = []
        if total_num > 0:
            raw_ul = soup.find("ul", class_="sellListContent")
            if raw_ul is None:
                raise ParseError("can not find house list: %s" % resp.url)

            li_list = raw_ul.find_all("li", class_="clear") or []

            for raw_li in li_list:
                try:
                    h_outer_id = raw_li.find("div", class_="btn-follow"
                                             )["data-hid"]
                    h_price = int(float(raw_li.find("div", class_="totalPrice")
                                        .span.get_text()))
                except Exception as e:
                    logger.exception(e)
                    raise ParseError("parse house outer_id or price failed: %s"
                                     % resp.url)

                position_info = raw_li.find("div", class_="positionInfo"
                                            ).get_text()
                rs = cls.p_build_year.search(position_info)
                if rs is None:
                    logger.warning("get house position failed: %s, %s" %
                                   (resp.url, h_outer_id))
                    h_build_year = h_floor = None
                else:
                    g_match = rs.groups()[0]
                    if g_match is None:
                        h_build_year = None
                    else:
                        h_build_year = int(g_match)
                    h_floor = position_info[:rs.span()[0]]

                house_info = raw_li.find("div", class_="houseInfo").get_text()
                rs = cls.p_area.search(house_info)
                if rs is None:
                    logger.warning("get house area failed: %s, %s" %
                                   (resp.url, h_outer_id))
                    h_area = 0
                else:
                    h_area = float(rs.groups()[0])

                houses_info.append({"outer_id": h_outer_id,
                                    "price": h_price,
                                    "build_year": h_build_year,
                                    "floor": h_floor,
                                    "area": h_area})

        if (total_page > 1
           and on_page == 1
           and len(houses_info) != number_per_page):
            raise ParseError("It seems that you set wrong number_per_page for"
                             "lianjia community: %s" % resp.url)

        return {"community_info": c_info,
                "houses_info": houses_info,
                "total_page": total_page}


class HouseLJ(IdMixin, Base):
    __tablename__ = 'house'

    community_id = Column(INTEGER, ForeignKey('community.id'), nullable=False)
    outer_id = Column(VARCHAR(128), nullable=False)

    area = Column(FLOAT)
    room = Column(VARCHAR(64))
    build_year = Column(INTEGER)
    floor = Column(VARCHAR(64))
    price_origin = Column(INTEGER)
    last_batch_number = Column(INTEGER)
    new = Column(BOOLEAN)
    available = Column(BOOLEAN)
    available_change_times = Column(INTEGER)

    price = Column(INTEGER)
    view_last_month = Column(INTEGER)
    view_last_week = Column(INTEGER)

    community = relationship('Community',
                             backref=backref('houses', order_by=view_last_month)
                             )
    SEARCH_URL = "http://sh.lianjia.com/ershoufang/%s.html"

    def __init__(self, outer_id, community, area=None, room=None,
                 build_year=None, floor=None, price=None,
                 last_batch_number=None):
        Base.__init__(self, outer_id=outer_id, community=community,
                      community_id=community.id, area=area, room=room,
                      build_year=build_year, floor=floor,
                      price=price, price_origin=price,
                      last_batch_number=last_batch_number,
                      new=True, available=True, available_change_times=0)

    def lj_search_request(self):
        url = self.SEARCH_URL % self.outer_id
        return Request(url=url, method="GET")

    def lj_parse(self, resp):

        soup = BeautifulSoup(resp.text, "html.parser")
        info = {}

        # check
        house_record = soup.find("div", "houseRecord").text
        if house_record.find(self.outer_id) < 0:
            raise ParseError("get house page with invalid outer_id, %s: %s"
                             % (house_record, resp.url))

        raw_div = soup.find("div", class_="panel")
        info["view_last_week"] = int(raw_div.find("div", class_="count").text)
        info["view_last_month"] = int(raw_div.find("span").text)

        return info


class CommunityRecordLJ(IdMixin, Base):
    __tablename__ = 'community_record'

    __table_args__ = (
        ForeignKeyConstraint(('batch_number', 'batch_type'),
                             ('batch_job.batch_number', 'batch_job.type')),
        Base.__table_args__
    )

    community_id = Column(INTEGER, ForeignKey('community.id'), nullable=False)
    batch_number = Column(INTEGER, nullable=False)
    batch_type = Column(BINARY(8), nullable=False)

    average_price = Column(INTEGER)
    house_available = Column(INTEGER)
    sold_last_season = Column(INTEGER)
    view_last_month = Column(INTEGER)

    new_number = Column(INTEGER)
    missing_number = Column(INTEGER)

    community = relationship('Community', foreign_keys=community_id,
                             backref=backref('community_records',
                                             order_by=batch_number))
    batch_job = relationship('BatchJobLJ',
                             foreign_keys=[batch_number, batch_type],
                             backref=backref('community_records',
                                             order_by=view_last_month))
    """
    house_records = relationship('HouseRecordLJ',
                                 primaryjoin='(CommunityRecordLJ.community_id==foreign(HouseRecordLJ.community_id)'
                                             ')&(CommunityRecordLJ.batch_job_id==HouseRecordLJ.batch_job_id)',
                                 )
"""
    def __init__(self, community, batch_job, **kwargs):
        Base.__init__(self, community=community, batch_job=batch_job, **kwargs)


class HouseRecordLJ(IdMixin, Base):
    __tablename__ = 'house_record'

    __table_args__ = (
        ForeignKeyConstraint(('batch_number', 'batch_type'),
                             ('batch_job.batch_number', 'batch_job.type')),
        Base.__table_args__
    )

    community_id = Column(INTEGER, ForeignKey('community.id'), nullable=False)
    house_id = Column(INTEGER, ForeignKey('house.id'), nullable=False)
    batch_number = Column(INTEGER, nullable=False)
    batch_type = Column(BINARY(8), nullable=False)

    price = Column(INTEGER)
    price_change = Column(INTEGER)
    view_last_month = Column(INTEGER)
    view_last_week = Column(INTEGER)

    community = relationship('Community', foreign_keys=community_id,
                             backref=backref('house_records',
                                             order_by=batch_number))
    batch_job = relationship('BatchJobLJ',
                             foreign_keys=[batch_number, batch_type],
                             backref=backref('house_records'))
    house = relationship('HouseLJ', backref=backref('house_records'),
                         foreign_keys=house_id)

    def __init__(self, house, batch_job, **kwargs):
        Base.__init__(self, house=house, community=house.community,
                      batch_job=batch_job, **kwargs)
        house.last_batch_number = batch_job.batch_number

    def load_detail(self, http_session):
        resp = utils.do_http_request(http_session,
                                     self.house.lj_search_request())
        info = self.house.lj_parse(resp)
        self.house.view_last_week = self.view_last_week = info["view_last_week"]
        self.house.view_last_month = self.view_last_month = info["view_last_month"]


class BatchJobLJ(BatchJob):
    __mapper_args__ = {
        'polymorphic_identity': b'lianjia' + bytes(1),
    }

    def _start(self, lj_number_per_page=None, community_outer_ids=None):
        result = base.FINISHED

        for community, job in self.get_community_and_job(community_outer_ids):
            if job is not None and job.status == base.FINISHED:
                continue
            elif job is None:
                job = CommunityJob(community, self)
                self.db_session.add(job)
                self.commit()

            logger.info("lianjia community start: %s" % community)
            try:
                job.start(self.db_session, self.http_session,
                          auto_commit=self.auto_commit,
                          lj_number_per_page=lj_number_per_page)
            except JobError as e:
                logger.error("CommunityJob of id %s failed: %s", job.id, e)
                job.status = base.FAILED
                self.commit()
                result = base.FAILED

        # is not able to put all the validation in the unit test,
        # so check the result when finished.
        if result == base.FINISHED:
            result = self.check_result()

        return result

    def get_community_and_job(self, community_outer_ids):
        if community_outer_ids is None:
            filter_ = None
        else:
            filter_ = CommunityLJ.outer_id.in_(community_outer_ids)

        return self._get_obj_and_job(CommunityLJ, CommunityJob,
                                     CommunityJob.community_id == CommunityLJ.id,
                                     filter_=filter_,
                                     order=CommunityLJ.outer_id)

    def check_result(self):
        # each community should has a record for this batch
        join_cond = (CommunityLJ.id == CommunityRecordLJ.community_id) & (
                     CommunityRecordLJ.batch_number == self.batch_number)
        null_num = (self.db_session.query(func.count(CommunityLJ.id))
                    .select_from(CommunityLJ)
                    .outerjoin(CommunityRecordLJ, join_cond)
                    .filter(CommunityRecordLJ.batch_number.is_(None))
                    .scalar())
        if null_num > 0:
            logger.error("failed to track some lianjia communities")
            return base.FAILED

        # number of house_record should equal the number of house that available
        num_record = (self.db_session.query(func.count(HouseRecordLJ.id))
                      .filter_by(batch_number=self.batch_number)
                      .scalar())
        num_house = (self.db_session.query(func.count(HouseLJ.id))
                     .filter(HouseLJ.available.is_(True))
                     .scalar())
        if num_record != num_house:
            logger.error("number of house_record and number of house that"
                         " available mismatch.")
            return base.FAILED

        # number of new house and missing house should match
        rs_cr = (self.db_session.query(func.sum(CommunityRecordLJ.new_number
                                                ).label("new_number"),
                                       func.sum(CommunityRecordLJ.missing_number
                                                ).label("missing_number"))
                 .filter_by(batch_number=self.batch_number)
                 .one())
        nc = func.sum(case([(HouseLJ.new.is_(True), 1)], else_=0)
                              ).label("new_number")
        mc = func.sum(
                case([(HouseLJ.last_batch_number == self.batch_number-1, 1)],
                     else_=0)).label("missing_number")
        rs_house = (self.db_session.query(nc, mc).one())

        if (rs_cr.new_number != rs_house.new_number
           or rs_cr.missing_number != rs_house.missing_number):
            logger.error("new_number or missing number mismatch.")
            return base.FAILED

        return base.FINISHED


class CommunityJob(JobWithCommunity):
    __mapper_args__ = {
        'polymorphic_identity': 'community_job_lj',
    }

    community = relationship(CommunityLJ,
                             foreign_keys=JobWithCommunity.community_id,
                             primaryjoin=JobWithCommunity.community_id==CommunityLJ.id)

    def _start(self, lj_number_per_page=None):

        existing_outer_ids = {h.outer_id: h for h in self.community.houses}
        search_func = functools.partial(self.community.lj_search,
                                        number_per_page=lj_number_per_page)

        for content in PagesIterator(self, search_func):
            c_info = content["community_info"]
            houses_info = content["houses_info"]

            if c_info is not None:
                # only page 1 will return community_info
                c_record = CommunityRecordLJ(self.community, self.batch_job,
                                             **c_info)
                self.community.update(**c_info)
                self.db_session.add(c_record)

            for h_info in houses_info:
                if h_info["outer_id"] in existing_outer_ids:
                    house = existing_outer_ids[h_info["outer_id"]]
                    price_change = h_info["price"] - house.price
                    # update house info
                    house.price = h_info["price"]
                    house.new = False
                    if not house.available:
                        house.available = True
                        house.available_change_times += 1
                else:
                    house = HouseLJ(h_info.pop("outer_id"), self.community,
                                    **h_info)
                    price_change = None

                # new house or house not check
                # the house may be checked more than once
                if (house.last_batch_number is None
                   or house.last_batch_number != self.batch_number):
                    h_record = HouseRecordLJ(house, self.batch_job,
                                             price=h_info["price"],
                                             price_change=price_change)
                    self.db_session.add(h_record)
                    h_record.load_detail(self.http_session)

            self.commit()

        # update the state of missing houses
        data = {HouseLJ.new: False,
                HouseLJ.available: False,
                HouseLJ.available_change_times:
                    HouseLJ.available_change_times + 1}
        (self.db_session.query(HouseLJ)
         .filter_by(community_id=self.community.id)
         .filter(HouseLJ.last_batch_number < self.batch_number)
         .update(data))

        # new number and miss number
        # warning: pymysql will convert result of sum(boolean) to boolean
        col_new = func.sum(func.convert(HouseLJ.new, text("INTEGER"))
                           ).label("new")
        col_missing = func.sum(
                        case([(HouseLJ.last_batch_number == self.batch_number-1, 1)],
                             else_=0)
                      ).label("missing")

        query = (select([col_new, col_missing])
                 .where(HouseLJ.community_id == self.community_id))
        rs = self.db_session.execute(query).fetchall()[0]

        data = {CommunityRecordLJ.new_number: rs.new,
                CommunityRecordLJ.missing_number: rs.missing}
        (self.db_session.query(CommunityRecordLJ)
         .filter_by(batch_number=self.batch_number,
                    community_id=self.community.id)
         .update(data))

        return base.FINISHED


__all__ = ['CommunityLJ', 'HouseLJ', 'CommunityRecordLJ', 'HouseRecordLJ',
           'BatchJobLJ', 'CommunityJob']
