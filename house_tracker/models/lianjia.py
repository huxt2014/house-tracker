# coding=utf-8

import re
import math
import time
import logging

from bs4 import BeautifulSoup
from sqlalchemy import Column, ForeignKey, func
from sqlalchemy.dialects.mysql import VARCHAR, INTEGER, BOOLEAN, FLOAT
from sqlalchemy.orm import relationship, backref, joinedload

from .base import BaseMixin, Base, Community, BatchJob, Job, JobWithCommunity

logger = logging.getLogger(__name__)


class CommunityLJ(Community):

    average_price = Column(INTEGER)
    house_available = Column(INTEGER)
    sold_last_season = Column(INTEGER)
    view_last_month = Column(INTEGER)
    last_batch_number = Column(INTEGER)
    valid_average_price = Column(INTEGER)
    # houses=relationship
    # community_records=relationship
    # house_records = relationship

    __mapper_args__ = {
        'polymorphic_identity': 'lianjia',
    }

    def __init__(self, name, outer_id, area):
        Community.__init__(self, name, outer_id, area.district, area=area)

    def __str__(self):
        return self.name

    def community_url(self, page):
        return 'http://sh.lianjia.com/ershoufang/d%sq%ss8' % (
            page, self.outer_id)

    def update(self, **kwargs):
        for key in ('average_price', 'house_available', 'sold_last_season',
                    'view_last_month'):
            if key in kwargs:
                setattr(self, key, kwargs[key])

    def parse_page(self, content, page_number):
        house_ids = []
        soup = BeautifulSoup(content, 'html.parser')

        try:
            if content.find(u'暂时没有找到符合条件的内容') > 0:
                return None, None
            # check page index
            on_page = int(soup.find('div', class_='page-box house-lst-page-box')
                          .find('a', class_='on').get_text())
            if page_number != on_page:
                raise Exception('target page %s, but get page %s' % (
                    page_number, on_page))

            # get community info
            if on_page == 1:
                c_info = {}
                li_tags = soup.find('div', 'secondcon fl').ul.find_all('li')
                try:
                    c_info['average_price'] = int(li_tags[0].find('strong')
                                                  .get_text())
                except AttributeError:
                    if li_tags[0].find('span', class_='newstrong'
                                       ).get_text() == u'暂无均价':
                        c_info['average_price'] = None
                    else:
                        msg = ('parse community average price failed: '
                               '%s->%s.') % (self.id, self.outer_id)
                        raise Exception(msg)
                c_info['house_available'] = int(li_tags[2].find('strong')
                                                .get_text())
                c_info['sold_last_season'] = int(li_tags[4].find('strong')
                                                 .get_text())
                c_info['view_last_month'] = int(li_tags[6].find('strong')
                                                .get_text())
            else:
                c_info = None

            # get house_id
            li_tags = soup.find('ul', id='house-lst',
                                class_='house-lst').find_all('li')
            for house_tag in li_tags:
                house_ids.append(house_tag.div.a['key'])

            return c_info, house_ids

        except Exception as e:
            logger.exception(e)
            raise Exception('parse community page error.')


class HouseLJ(BaseMixin, Base):
    __tablename__ = 'house'

    community_id = Column(INTEGER, ForeignKey('community.id'), nullable=False)
    outer_id = Column(VARCHAR(128), nullable=False)

    area = Column(FLOAT)
    room = Column(VARCHAR(64))
    build_year = Column(INTEGER)
    floor = Column(VARCHAR(64))
    available = Column(BOOLEAN)

    price_origin = Column(INTEGER)
    price = Column(INTEGER)
    view_last_month = Column(INTEGER)
    view_last_week = Column(INTEGER)
    new = Column(BOOLEAN)
    available_change_times = Column(INTEGER)

    last_batch_number = Column(INTEGER)

    community = relationship('Community',
                             backref=backref('houses', order_by=view_last_month)
                             )

    def __init__(self, outer_id, community):
        Base.__init__(self, outer_id=outer_id, community=community,
                      community_id=community.id, new=True, available=True,
                      available_change_times=0)

    def download_url(self):
        return 'http://sh.lianjia.com/ershoufang/%s.html' % self.outer_id

    def parse_page(self, content):
        soup = BeautifulSoup(content, 'html.parser')
        assert soup.find('span', class_='title',
                         string=u'房源编号：' + self.outer_id
                         ) is not None, 'get wrong page'

        h_info = {}
        house_info_tag = soup.find('div', class_='houseInfo')
        around_info_tag = soup.find('table', class_='aroundInfo')

        h_info['price'] = int(house_info_tag.div.div.get_text()
                              .replace(u'万', ''))
        h_info['room'] = (house_info_tag.find('div', class_='room').div
                          .get_text())
        h_info['area'] = float(house_info_tag.find('div', class_='area')
                               .div.get_text().replace(u'平', ''))
        year_string = (around_info_tag.find('span', string=re.compile(u'年代'))
                       .parent.text.split(u'：')[1].strip())
        result = re.match('(\d{4})', year_string)
        if result:
            h_info['build_year'] = int(result.group(1))
        else:
            h_info['build_year'] = None
        h_info['floor'] = (around_info_tag.find('span',
                                                string=re.compile(u'楼层'))
                           .parent.text.split(u'：')[1].strip())

        tmp_tag = soup.find('div', string=u'近7天带看次数')
        if tmp_tag:
            view_tag = tmp_tag.parent
            h_info['view_last_week'] = int(view_tag.find(class_='count').text)
            h_info['view_last_month'] = int(view_tag.find('span').text)
        else:
            # if house is not available now, the following information will
            # not display.
            h_info['view_last_week'] = None
            h_info['view_last_month'] = None

        return h_info


class CommunityRecordLJ(BaseMixin, Base):
    __tablename__ = 'community_record'

    def __init__(self, community, batch_job, **kwargs):
        Base.__init__(self, community=community, batch_job=batch_job)
        for key, value in kwargs.iteritems():
            if hasattr(self, key):
                setattr(self, key, value)

    community_id = Column(INTEGER, ForeignKey('community.id'), nullable=False)
    batch_job_id = Column(INTEGER, ForeignKey('batch_job.id'))

    average_price = Column(INTEGER)
    house_available = Column(INTEGER)
    sold_last_season = Column(INTEGER)
    view_last_month = Column(INTEGER)

    rise_number = Column(INTEGER)
    reduce_number = Column(INTEGER)
    valid_unchange_number = Column(INTEGER)
    new_number = Column(INTEGER)
    miss_number = Column(INTEGER)
    view_last_week = Column(INTEGER)
    valid_average_price = Column(INTEGER)
    average_price_change = Column(INTEGER)

    batch_number = Column(INTEGER)

    community = relationship('Community', foreign_keys=community_id,
                             backref=backref('community_records',
                                             order_by=view_last_month))
    batch_job = relationship('BatchJobLJ', foreign_keys=batch_job_id,
                             backref=backref('community_records',
                                             order_by=view_last_month))
    house_records = relationship('HouseRecordLJ',
                                 primaryjoin='(CommunityRecordLJ.community_id==foreign(HouseRecordLJ.community_id)'
                                             ')&(CommunityRecordLJ.batch_job_id==HouseRecordLJ.batch_job_id)',
                                 )


class HouseRecordLJ(BaseMixin, Base):
    __tablename__ = 'house_record'

    community_id = Column(INTEGER, ForeignKey('community.id'), nullable=False)
    house_id = Column(INTEGER, ForeignKey('house.id'), nullable=False)
    batch_job_id = Column(INTEGER, ForeignKey('batch_job.id'))

    price = Column(INTEGER)
    price_change = Column(INTEGER)
    view_last_month = Column(INTEGER)
    view_last_week = Column(INTEGER)

    batch_number = Column(INTEGER)

    community = relationship('Community', foreign_keys=community_id,
                             backref=backref('house_records',
                                             order_by=batch_job_id))
    batch_job = relationship('BatchJobLJ', foreign_keys=batch_job_id,
                             backref=backref('house_records'))
    house = relationship('HouseLJ', backref=backref('house_records'),
                         foreign_keys=house_id)

    def __init__(self, house, batch_job, **kwargs):
        Base.__init__(self, house=house, community=house.community,
                      batch_job=batch_job)
        for key, value in kwargs.iteritems():
            if hasattr(self, key):
                setattr(self, key, value)


class BatchJobLJ(BatchJob):
    __mapper_args__ = {
        'polymorphic_identity': 'lianjia',
    }

    def _initial(self, session):
        communities = session.query(CommunityLJ).all()
        for c in communities:
            logger.debug('initial community batch jobs: %s -> %s',
                         c.id, c.outer_id)
            job = CommunityJobLJ(c, self, 1)
            first_page = job.get_web_page(cache=True)
            c_info, house_ids = c.parse_page(first_page, 1)
            if not house_ids:
                logger.warn('no house found for community: id=%s, outer_id=%s',
                            c.id, c.outer_id)
                job.parameters['no_house'] = True
            else:
                total_page = int(math.ceil(c_info['house_available'] / 20.0))
                for i in range(total_page - 1):
                    session.add(CommunityJobLJ(c, self, i + 2))

            if not job.use_cached:
                time.sleep(self.job_args.get('interval_time', 0.1))

    def _start(self, session):
        communities = (session.query(CommunityLJ)
                       .filter(CommunityLJ.last_batch_number
                               != self.batch_number)
                       .all())
        for c in communities:
            self.finish_one_community(c, session)

    def finish_one_community(self, community, session):
        for cls in (CommunityJobLJ, HouseJobLJ):
            jobs = (session.query(cls)
                    .filter_by(community_id=community.id,
                               batch_job_id=self.id)
                    .filter(CommunityJobLJ.status.in_(['ready', 'retry']))
                    .all())
            for job in jobs:
                job.start(session, **self.job_args)

        # update the state of missing houses
        data = {HouseLJ.new: False,
                HouseLJ.available: False,
                HouseLJ.available_change_times:
                    HouseLJ.available_change_times + 1}
        (session.query(HouseLJ)
         .filter_by(last_batch_number=self.batch_number - 1,
                    community_id=community.id)
         .update(data))

        # simple aggregation
        sql = """
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
          and T2.batch_job_id = :batch_job_id
        where T1.community_id = :community_id"""
        rs = session.execute(sql, {'current_batch': self.batch_number,
                                   'community_id': community.id,
                                   'batch_job_id': self.id}
                             ).fetchall()[0]

        c_record = (session.query(CommunityRecordLJ)
                    .options(joinedload('house_records').joinedload('house'),
                             joinedload('community'))
                    .filter_by(batch_job_id=self.id,
                               community_id=community.id)
                    .one())

        (c_record.rise_number, c_record.reduce_number,
         c_record.valid_unchange_number, c_record.new_number,
         c_record.miss_number, c_record.view_last_week) = rs

        # get average price
        house_avg_prices = [house_record.price * 10000 / house_record.house.area
                            for house_record in c_record.house_records
                            if (house_record.view_last_week > 0
                                or house_record.view_last_month > 0
                                or house_record.price_change
                                or house_record.house.new)]
        if house_avg_prices:
            c_record.valid_average_price = int(sum(house_avg_prices) /
                                               len(house_avg_prices))
            community.valid_average_price = c_record.valid_average_price

        # get average price change
        last_batch_record = (session.query(CommunityRecordLJ)
                             .join(CommunityRecordLJ.batch_job)
                             .filter(CommunityRecordLJ.community_id ==
                                     c_record.community_id)
                             .filter(BatchJobLJ.batch_number ==
                                     self.batch_number - 1)
                             .first())
        if (last_batch_record
            and c_record.valid_average_price is not None
            and last_batch_record.valid_average_price is not None):
            c_record.average_price_change = (
                c_record.valid_average_price -
                last_batch_record.valid_average_price)

        community.last_batch_number = self.batch_number
        session.commit()


class CommunityJobLJ(JobWithCommunity):
    __mapper_args__ = {
        'polymorphic_identity': 'community_job_lianjia',
    }

    def __init__(self, community, batch_job, page):
        JobWithCommunity.__init__(self, community, batch_job,
                                  parameters={'page': page})

    def inner_start(self, session, **kwargs):
        page = self.parameters['page']

        content = self.get_web_page()
        c_info, house_ids = self.community.parse_page(content, page)

        if page == 1:
            if self.parameters.get('no_house'):
                # when no house found, parse_page return (None, None)
                c_info = {'house_available': 0}

            c_record = CommunityRecordLJ(self.community, self.batch_job,
                                         **c_info)
            self.community.update(**c_info)
            session.add(c_record)

        if self.parameters.get('no_house'):
            return

        for house_id in house_ids:
            house = session.query(HouseLJ).filter_by(outer_id=house_id).first()
            job_exist = (session.query(func.count(HouseJobLJ.id))
                         .join(HouseJobLJ.house)
                         .filter(HouseJobLJ.batch_job_id == self.batch_job.id)
                         .filter(HouseLJ.outer_id == house_id)
                         .scalar())
            if not house:
                house = HouseLJ(house_id, self.community)
            else:
                if job_exist:
                    # house status already be updated
                    pass
                else:
                    house.new = False
                    if not house.available:
                        house.available = True
                        house.available_change_times += 1

            # skip duplicate job
            if not job_exist:
                session.add(HouseJobLJ(house, self.batch_job))

    def web_uri(self):
        return self.community.community_url(self.parameters['page'])

    def disk_uri(self):
        return '%s-%s-%s.html' % (self.community.id, self.community.outer_id,
                                  self.parameters['page'])


class HouseJobLJ(JobWithCommunity):
    __mapper_args__ = {
        'polymorphic_identity': 'house_job_lianjia',
    }

    house_id = Column(INTEGER, ForeignKey('house.id'))
    house = relationship('HouseLJ', backref=backref('jobs'),
                         foreign_keys=house_id)

    def __init__(self, house, batch_job):
        JobWithCommunity.__init__(self, house.community, batch_job)
        self.house = house

    def inner_start(self, session, **kwargs):
        house_info = self.house.parse_page(self.get_web_page())
        h_record = HouseRecordLJ(self.house, self.batch_job, **house_info)
        if self.house.new:
            for key in ('room', 'area', 'floor', 'build_year'):
                setattr(self.house, key, house_info[key])
            self.house.price_origin = house_info['price']
        else:
            h_record.price_change = h_record.price - self.house.price
        for key in ('price', 'view_last_week', 'view_last_month'):
            setattr(self.house, key, house_info[key])

        self.house.last_batch_number = self.batch_job.batch_number
        self.house.available = True
        session.add(h_record)

    def web_uri(self):
        return self.house.download_url()


__all__ = ['CommunityLJ', 'HouseLJ', 'CommunityRecordLJ', 'HouseRecordLJ',
           'BatchJobLJ', 'CommunityJobLJ', 'HouseJobLJ']
