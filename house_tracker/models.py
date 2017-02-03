# coding=utf-8

import re
import os
import math
import time
import json
import urllib
import random
import logging
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup
from sqlalchemy import Column, ForeignKey, types, inspect
from sqlalchemy.dialects.mysql import (VARCHAR, INTEGER, BOOLEAN, DATETIME, 
                                       FLOAT, DATE, TEXT)
from sqlalchemy.orm import relationship, backref, joinedload
from sqlalchemy.ext.declarative import (
        declarative_base, declared_attr, DeclarativeMeta)

from house_tracker.exceptions import JobError, BatchJobError


logger = logging.getLogger(__name__)
Base = declarative_base()


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
    outer_id_fd = Column(INTEGER)
    # jobs = relationship('DistrictJob')
    
    def fd_search_url(self, page_num):
        return ('http://www.fangdi.com.cn/complexpro.asp?page=%s&districtID=%s'
                '&Region_ID=&projectAdr=&projectName=&startCod=&buildingType=1'
                '&houseArea=0&averagePrice=0&selState=&selCircle=0'
                ) % (page_num, self.outer_id_fd)

    def parse_fd_html(self, content, page_index):
        soup = BeautifulSoup(content, 'html.parser')
        community_list = []
        
        for table in soup.find_all('table'):
            target_cols = table.tr.find_all('td', string=['项目地址', '所在区县'], 
                                            recursive=False)
            if len(target_cols) == 2:
                break
        else:
            raise Exception('parse error')
        
        try:
            # parse community row
            for row in table.find_all('tr'):
                if not row.get('valign'):
                    continue
                tds = row.find_all('td', recursive=False)
        
                if self.name != unicode(tds[5].string):
                    logger.error('%s, %s', self.name, unicode(tds[5].string))
                    raise Exception
                
                outer_id = (tds[1].a['href'].split('projectID=')[1]
                            .decode('base64').split('|')[0])
                c_info = {'outer_id': outer_id,
                          'name': tds[1].get_text(),
                          'location': unicode(tds[2].string),
                          'total_number': int(tds[3].string),
                          'total_area': float(tds[4].string)}
                community_list.append(c_info)
            
            # parse page number
            sub_table = table.table
            result = re.search(u'第(\d+)页/共(\d+)页', 
                               sub_table.tr.td.td.get_text())
            current_page = int(result.group(1))
            total_page = int(result.group(2))
            if current_page != page_index:
                raise Exception
        except Exception as e:
            logger.exception(e)
            raise Exception('parse error')
        
        if not community_list:
            raise Exception('parse error')
        
        return community_list, total_page


class Area(BaseMixin, Base):
    __tablename__ = 'area'
    
    name = Column(VARCHAR(64))
    district_id = Column(INTEGER, ForeignKey("district.id"))
    
    district = relationship('District')


class LandSoldRecord(BaseMixin, Base):
    __tablename__ = 'land_sold_record'
    
    district_id = Column(INTEGER, ForeignKey("district.id"))
    area_id = Column(INTEGER, ForeignKey("area.id"))
    
    land_name = Column(VARCHAR(256))
    description = Column(TEXT)
    boundary = Column(TEXT)
    record_no = Column(VARCHAR(64))
    sold_date = Column(DATE)
    land_price = Column(INTEGER)
    company = Column(VARCHAR(64))
    land_area = Column(INTEGER)
    plot_ratio = Column(VARCHAR(64))
    status = Column(VARCHAR(64))
    
    def web_uri(self):
        return 'http://www.shtdsc.com/bin/crjg/v/%s' % self.record_no
    
    def parse_web_page(self, content):
        soup = BeautifulSoup(content, 'html.parser')
        tag_text = soup.find('script', string=re.compile('data:')).text
        left = tag_text.find('data:') + 5
        right = -1
        for i in range(4):
            right = tag_text.rfind('}', 0, right)
        
        data = json.loads(tag_text[left:right+1].strip())['data'][0]
        if data['dkggh'] != self.record_no:
            raise Exception('parse error: %s, %s' % (
                                    self.record_no, data['dkggh']))
        
        return data
    

class Land(BaseMixin, Base):
    __tablename__ = 'land'
    
    sold_record_id = Column(INTEGER, ForeignKey("land_sold_record.id"))
    district_id = Column(INTEGER, ForeignKey("district.id"))
    area_id = Column(INTEGER, ForeignKey("area.id"))
    
    description = Column(VARCHAR(64))
    plot_ratio = Column(FLOAT)
    type = Column(VARCHAR(64), nullable=False)
    
    sold_record = relationship(LandSoldRecord, foreign_keys=sold_record_id)
    district = relationship(District, foreign_keys=district_id)
    
    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'land',
        }


class LandResidential(Land):
    __mapper_args__ = {
        'polymorphic_identity': 'residential',
        }


class LandSemiResidential(Land):
    __mapper_args__ = {
        'polymorphic_identity': 'semi-residential',
        }


class LandRelocation(Land):
    __mapper_args__ = {
        'polymorphic_identity': 'relocation',
        }
    

class Community(BaseMixin, Base):
    __tablename__ = 'community'
    
    district_id = Column(INTEGER, ForeignKey("district.id"))
    area_id = Column(INTEGER, ForeignKey('area.id'))
    outer_id = Column(VARCHAR(128))
    name = Column(VARCHAR(64), nullable=False)
    area_name = Column(VARCHAR(64))
    type = Column(VARCHAR(64), nullable=False)
    # jobs=relationship
    area = relationship('Area', foreign_keys=area_id)
    
    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'community',
        }

    district = relationship('District')


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
        Community.__init__(self, name=name, outer_id=outer_id,
                           area=area, district=area.district)
    
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
                c_info=None
            
            # get house_id
            li_tags = soup.find('ul', id='house-lst', 
                                class_='house-lst').find_all('li')
            for house_tag in li_tags:
                house_ids.append(house_tag.div.a['key'])
                
            return c_info, house_ids
            
        except Exception as e:
            logger.exception(e)
            raise Exception('parse community page error.')
        

class CommunityFD(Community):
    total_number = Column(INTEGER)
    total_area = Column(FLOAT)
    location = Column(VARCHAR(1024))
    track_presale = Column(BOOLEAN, default=True)
    presale_url_name = Column(VARCHAR(1024))
    company = Column(VARCHAR(1024))
    # presales = relationship('PresalePermit')
    
    __mapper_args__ = {
        'polymorphic_identity': 'fangdi',
        }
    
    def presale_url(self):
        project_name = (self.presale_url_name or self.name).encode('gbk')
        query = urllib.urlencode({'projectname': project_name})
        return 'http://www.fangdi.com.cn/Presell.asp?projectID=%s&%s'%(
                    self.tmp_id, query)
        
    def community_url(self):
        return 'http://www.fangdi.com.cn/proDetail.asp?projectID=%s' % self.tmp_id
        
    @property
    def tmp_id(self):
        today = date.today().isoformat().replace('-0', '-')
        tail = random.randint(1, 99)
        tmp_id= ('%s|%s|%s' % (self.outer_id, today, tail))
        return tmp_id.encode('base64').strip()
        
    def parse_presale_page(self, content):
        soup = BeautifulSoup(content, 'html.parser')
        table = soup.table
        test_cols = table.tr.find_all('td', string=['开盘日期', '总套数'], 
                                      recursive=False)
        if len(test_cols) != 2:
            raise Exception('parse error')
        
        presale_list = []
        for row in table.find_all('tr', recursive=False):
            if not row.get('onclick'):
                continue
            tds = row.find_all('td', recursive=False)
            
            try:
                sale_date = date(*(int(e) for e in tds[2].get_text().split('-')))
            except Exception as e:
                # date may be null
                logger.warn('%s get date failed: %s', self.id, e.__str__())
                sale_date=date.today()
                
            try:
                status = tds[7].get_text()
            except Exception as e:
                # status may be null
                logger.warn('%s get status failed: %s', self.id, e.__str__())
                status = None
            
            presale_list.append(
                {'serial_number': tds[0].get_text(),
                 'description': tds[1].get_text(),
                 'sale_date': sale_date,
                 'total_number': int(tds[3].get_text()),
                 'normal_number': int(tds[4].get_text()),
                 'total_area': tds[5].get_text(),
                 'normal_area': float(tds[6].get_text().split(' ')[0]),
                 'status': status
                 })
        
        if not presale_list:
            raise Exception('parse error.')
        
        return presale_list
    
    def parse_community_page(self, content):
        soup = BeautifulSoup(content, 'html.parser')
        
        for table in soup.find_all('table'):
            try:
                if table.tr.td.get_text() == u'项目名称：':
                    break
            except Exception:
                continue
        else:
            raise Exception('fangdi parse error: %s' % self.id)
        
        c_info = {}
        trs = table.find_all('tr', recursive=False)
        
        # get area
        tds = trs[1].find_all('td', recursive=False)
        c_info['area_name'] = tds[3].get_text()
        
        # get company
        tds = trs[2].find_all('td', recursive=False)
        c_info['company'] = tds[1].get_text()
        
        c_info['presale_url_name'] = soup.iframe['src'].split('projectname=')[1]
        
        return c_info


class HouseLJ(BaseMixin, Base):
    __tablename__ = 'house'
    
    community_id = Column(INTEGER, ForeignKey('community.id'), nullable=False)
    outer_id = Column(VARCHAR(128), nullable=False)
    
    area = Column(FLOAT)
    room = Column(VARCHAR(64))
    build_year = Column(INTEGER)
    floor = Column(VARCHAR(64))
    available = Column(BOOLEAN, default=True)
    
    price_origin = Column(INTEGER)
    price = Column(INTEGER)
    view_last_month = Column(INTEGER)
    view_last_week = Column(INTEGER)
    new = Column(BOOLEAN, default=True)
    available_change_times = Column(INTEGER, default=0)
    
    last_batch_number = Column(INTEGER)
  
    community = relationship('Community', 
                             backref=backref('houses', order_by=view_last_month)
                             )
    
    def __init__(self, outer_id, community):
        Base.__init__(self, outer_id=outer_id, community=community)
    
    def download_url(self):
        return 'http://sh.lianjia.com/ershoufang/%s.html' % self.outer_id
    
    def parse_page(self, content):
        soup = BeautifulSoup(content, 'html.parser')
        assert soup.find('span', class_='title', 
                         string = u'房源编号：'+self.outer_id
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


class PresalePermit(BaseMixin, Base):
    __tablename__ = 'presale_permit'
    
    community_id = Column(INTEGER, ForeignKey('community.id'), nullable=False)
    community = relationship('CommunityFD', backref=backref('presales'),
                             foreign_keys=community_id)
    
    serial_number = Column(VARCHAR(32))
    description = Column(VARCHAR(1024))
    sale_date = Column(DATE)
    total_number = Column(INTEGER)
    normal_number = Column(INTEGER)
    total_area = Column(FLOAT)
    normal_area = Column(FLOAT)
    status = Column(VARCHAR(64))


class BatchJob(BaseMixin, Base):
    __tablename__ = 'batch_job'
    
    batch_number = Column(INTEGER, nullable=False)
    status = Column(VARCHAR(64))
    type = Column(VARCHAR(64), nullable=False)
    
    jobs_unsuccessful = relationship('Job', foreign_keys='Job.batch_job_id',
                                     primaryjoin="(BatchJob.id==Job.batch_job_id)"
                                     "&(Job.status!='succeed')")
    
    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'batch_job',
    }
    
    def __init__(self,  batch_number):
        '''
        '''
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
        
    def initial(self, *args, **kwargs):
        raise NotImplementedError
    
    def start(self, *args, **kwargs):
        raise NotImplementedError


class BatchJobFD(BatchJob):
    
    __mapper_args__ = {
        'polymorphic_identity': 'fangdi',
    }
    
    def __str__(self):
        return '%s-th batch job for fangdi.com' % self.batch_number
    
    def initial(self, session, **kwargs):
        self.before_act(**kwargs)
        
        try:
            for d in session.query(District).all():
                job = DistrictJob(d, 1, self)
                session.add(job)
                first_page = job.get_web_page(cache=True)
                cs, total_page = job.district.parse_fd_html(first_page, 1)
                for i in range(total_page-1):
                    session.add(DistrictJob(d, i+2, self))
        except Exception as e:
            session.rollback()
            logger.error('%s failed.', self)
            logger.exception(e)
        else:
            session.commit()
            logger.info('%s finish', self)
        
    def start(self, session, **kwargs):
        self.before_act(**kwargs)
        
        c_set = (session.query(CommunityFD.outer_id, CommunityFD.track_presale)
                 .all())
        notrack_outer_ids = set(c.outer_id for c in c_set
                                if not c.track_presale)
        
        (session.query(Job)
         .filter_by(batch_job_id=self.id, status='failed')
         .update({Job.status: 'retry'}) )
    
        try:
            for job_cls in (DistrictJob, CommunityJobFD, PresaleJob):
                jobs = (session.query(job_cls)
                        .filter_by(batch_job_id=self.id)
                        .filter(Job.status.in_(('ready', 'retry')))
                        .all())
                for job in jobs:
                    if job_cls is DistrictJob:
                        job.start(session, notrack_outer_ids=notrack_outer_ids,
                                  **self.job_args)
                    else:
                        job.start(session, **self.job_args)
                
        except Exception as e:
            logger.exception(e)
            logger.error('%s failed', self)
        else:
            logger.info('%s finish.', self)


class BatchJobLJ(BatchJob):
    __mapper_args__ = {
        'polymorphic_identity': 'lianjia',
    }
    
    def initial(self, session, **kwargs):
        self.before_act(**kwargs)
        
        communities = session.query(CommunityLJ).all()
        for c in communities:
            logger.debug('initial community batch jobs: %s -> %s', 
                         c.id, c.outer_id)
            job = CommunityJobLJ(c, 1, self)
            first_page = job.get_web_page(cache=True)
            c_info, house_ids = c.parse_page(first_page, 1)
            if not house_ids:
                logger.warn('no house found for community: id=%s, outer_id=%s',
                            c.id, c.outer_id)
                job.community = None
                session.expunge(job)
                continue
            total_page = int(math.ceil(c_info['house_available']/20.0))
            session.add(job)
            for i in range(total_page-1):
                session.add(CommunityJobLJ(c, i+2, self))
            
            if not job.use_cached:
                time.sleep(self.job_args.get('interval_time', 0.5))

    def start(self, session, **kwargs):
        self.before_act(**kwargs)
        
        (session.query(Job)
         .filter_by(batch_job_id=self.id, status='failed')
         .update({Job.status: 'retry'}))
        
        for c in session.query(CommunityLJ).all():
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
        (session.query(HouseLJ)
                .filter_by(last_batch_number=self.batch_number-1,
                           community_id=community.id)
                .update(
            {HouseLJ.new: False,
             HouseLJ.available: False,
             HouseLJ.available_change_times: HouseLJ.available_change_times+1})
         )

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
        house_avg_prices = [house_record.price * 10000/house_record.house.area
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
                             .join(BatchJobLJ,
                                   (CommunityRecordLJ.batch_job_id ==
                                    BatchJobLJ.id))
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

        logger.info('finish')
        session.commit()


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
    status = Column(VARCHAR(64), default='ready')
    target_uri = Column(VARCHAR(1024))
    parameters = Column(PickleType, default={})
    type = Column(VARCHAR(64))
    
    batch_job = relationship(BatchJob, foreign_keys=batch_job_id,
                             backref=backref('jobs'))
    
    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'job',
        }
    
    use_cached = False
    
    def start(self, session, auto_commit=True, clean_cache=False, 
              interval_time=None, **kwargs):
        if clean_cache:
            # clean cached file before job start
            path = self.cache_file_path
            if path and os.path.isfile(path):
                logger.debug('remove cached file: %s', path)
                os.remove(path)
        
        if self.status not in ('ready', 'retry'):
                raise Exception('job status error')
        try:
            self.inner_start(session, **kwargs)
        except (Exception, KeyboardInterrupt) as e:
            logger.exception(e)
            session.rollback()
            (session.query(Job)
                    .filter_by(id=self.id)
                    .update({Job.status: 'failed',
                             Job.target_uri: self.web_uri()}))
            raise
        else:
            self.status='succeed'
        finally:
            if auto_commit:
                session.commit()
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
            raise Exception('get cache file path failed.')
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
                if try_times <3:
                    logger.warn(e)
                    logger.warn('%s-th try failed: %s.', try_times, url)
                    time.sleep(3)
                else:
                    logger.exception(e)
                    logger.error('get html failed: %s', url)
                    raise Exception('http request error: %s', url)
            else:
                break
                
        if res.status_code != 200:
            logger.error('bad response: %s, %s', res.status_code, 
                         self.url)
            raise Exception
        res.encoding = self.web_encode()
        
        if cache:
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
        """return cache_dir/disk_uri. cache_dir is retrieved from environ,
        disk_uri is got by method self.disk_uri.
        """
        if not hasattr(self, '_cache_file_path'):
            if not hasattr(self, 'disk_uri'):
                self._cache_file_path = None
            else:
                cache_dir = (self.batch_job.cache_dir if self.batch_job
                             else '/tmp')
                self._cache_file_path = os.path.join(cache_dir, self.disk_uri())        
        return self._cache_file_path


class DistrictJob(Job):

    district_id = Column(INTEGER, ForeignKey('district.id'))
    district = relationship('District', foreign_keys=district_id)
    __mapper_args__ = {
        'polymorphic_identity': 'district_job',
        }
    
    def __init__(self, district, page, batch_job):
        Job.__init__(self, district=district, batch_job=batch_job,
                     parameters={'page': page})
        
    def inner_start(self, session, notrack_outer_ids=None):
        page_index = self.parameters['page']
        content = self.get_web_page()
        community_list, total_page = self.district.parse_fd_html(
                                                       content, page_index)
        # add community job
        for c_info in community_list:
            if c_info['outer_id'] in notrack_outer_ids:
                continue
            community = (session.query(CommunityFD)
                                .filter_by(outer_id=c_info['outer_id'])
                                .first())
            if not community:
                community = CommunityFD(district=self.district,
                                        **c_info)
                session.add(community)
                session.add(CommunityJobFD(community, self.batch_job))
            
            session.add(PresaleJob(community, self.batch_job))

    def web_uri(self):
        page_index = self.parameters['page']
        return self.district.fd_search_url(page_index)
    
    def web_encode(self):
        return 'gbk'
    
    def disk_uri(self):
        return 'fd-district-%s-%s-%s.html' % (
                    self.district.id, self.district.outer_id_fd,
                    self.parameters['page'])


class CommunityJob(Job):
    community_id = Column(INTEGER, ForeignKey('community.id'))
    community = relationship('Community', backref=backref('jobs'),
                             foreign_keys=community_id)
    __mapper_args__ = {
        'polymorphic_identity': 'community_job',
        }


class CommunityJobFD(CommunityJob):
    
    __mapper_args__ = {
        'polymorphic_identity': 'community_job_fangdi',
        }
    
    def __init__(self, community, batch_job=None):
        Job.__init__(self, community=community, batch_job=batch_job)
    
    def inner_start(self, session):    
        content = self.get_web_page()
        c_info = self.community.parse_community_page(content)
        
        for key, value in c_info.iteritems():
            setattr(self.community, key, value)
    
    def web_uri(self):
        return self.community.community_url()
    
    def web_encode(self):
        return 'gbk'


class CommunityJobLJ(CommunityJob):
    __mapper_args__ = {
        'polymorphic_identity': 'community_job_lianjia',
        }
    
    def __init__(self, community, page, batch_job):
        Job.__init__(self, community=community, batch_job=batch_job,
                     parameters={'page': page})
    
    def inner_start(self, session, **kwargs):
        page = self.parameters['page']
        
        content = self.get_web_page()
        c_info, house_ids = self.community.parse_page(content, page)
            
        if page == 1:
            c_record = CommunityRecordLJ(self.community, self.batch_job,
                                         **c_info)
            self.community.update(**c_info)
            self.community.last_batch_number = self.batch_job.batch_number
            session.add(c_record)
                
        for house_id in house_ids:
            house = session.query(HouseLJ).filter_by(outer_id=house_id).first()
            if not house:
                house = HouseLJ(house_id, self.community)
            else:
                house.new = False
                if not house.available:
                    house.available = True
                    house.available_change_times += 1
                    
            session.add(house)
            session.add(HouseJobLJ(house, self.batch_job))
        
    def web_uri(self):
        return self.community.community_url(self.parameters['page'])
            
    def disk_uri(self):
        return '%s-%s-%s.html' % (self.community.id, self.community.outer_id,
                                  self.parameters['page'])


class HouseJobLJ(CommunityJob):
    __mapper_args__ = {
        'polymorphic_identity': 'house_job_lianjia',
        }
    
    house_id = Column(INTEGER, ForeignKey('house.id'))
    house = relationship('HouseLJ', backref=backref('jobs'),
                         foreign_keys=house_id)
    
    def __init__(self, house, batch_job):
        Job.__init__(self, batch_job=batch_job, house=house,
                     community=house.community)
    
    def inner_start(self, session):
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


class PresaleJob(CommunityJob):
    
    __mapper_args__ = {
        'polymorphic_identity': 'presale_job',
        }
    
    def __init__(self, community, batch_job):
        Job.__init__(self, community=community, batch_job=batch_job)
    
    def inner_start(self, session):
        content = self.get_web_page()
        presales = self.community.parse_presale_page(content)
        old_presales = set(p.serial_number for p in self.community.presales)
        
        for presale_info in presales:
            if not presale_info['serial_number'] in old_presales:
                presale_info['community_id'] = self.community.id
                session.add(PresalePermit(**presale_info))
        
    def web_uri(self):
        return self.community.presale_url()
    
    def web_encode(self):
        return 'gbk'

__all__ = [key for key in list(globals().keys())
           if isinstance(globals()[key], DeclarativeMeta)]    
