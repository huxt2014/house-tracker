# coding=utf-8

import re
import math
import time
import json
import urllib
import random
import logging
import requests
from datetime import date, datetime

from bs4 import BeautifulSoup
from sqlalchemy import Column, ForeignKey, types, inspect
from sqlalchemy.dialects.mysql import (VARCHAR, INTEGER, BOOLEAN, DATETIME, 
                                       FLOAT, DATE, TEXT)
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base, declared_attr 
from sqlalchemy.orm.collections import InstrumentedList


__all__ = ['District', 'Community', 'CommunityLJ', 'CommunityFD', 'House',
           'CommunityRecord', 'HouseRecord', 'PresalePermit', 'Job', 
           'DistrictJob',  'CommunityJob']

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
                
    def init_batch_jobs(self, session, batch_number):
        first_page_job = DistrictJob(district=self, 
                                     batch_number=batch_number, 
                                     target_url=self.fd_search_url(1),
                                     parameters={'page': 1})
        session.add(first_page_job)
        first_page_job.start(session)
                   
    def parse_html(self, content, page_index):
        soup = BeautifulSoup(content, 'html.parser')
        community_list = []
        total_page = None
        
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
        
class Community(BaseMixin, Base):
    __tablename__ = 'community'
    
    district_id = Column(INTEGER, ForeignKey("district.id"))
    outer_id = Column(VARCHAR(128))
    name = Column(VARCHAR(64), nullable=False)
    area = Column(VARCHAR(32))
    type = Column(VARCHAR(64), nullable=False)
    # jobs=relationship
    
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
    last_track_week = Column(INTEGER)
    
    __mapper_args__ = {
        'polymorphic_identity': 'lianjia',
        }
    
    def __str__(self):
        return self.name

class CommunityFD(Community):
    total_number = Column(INTEGER)
    total_area = Column(FLOAT)
    location = Column(VARCHAR(1024))
    
    __mapper_args__ = {
        'polymorphic_identity': 'fangdi',
        }
    
    def presale_url(self):
        query = urllib.urlencode({'projectname': self.name.encode('gb2312')})
        return 'http://www.fangdi.com.cn/Presell.asp?projectID=%s&%s'%(
                    self.tmp_id, query)
    
    @property
    def tmp_id(self):
        return self.generate_tmp_id()
    
    def generate_tmp_id(self):
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
                logger.warn(e)
                sale_date=None
            presale_list.append(
                {'serial_number': tds[0].get_text(),
                 'description': tds[1].get_text(),
                 'sale_date': sale_date,
                 'total_number': int(tds[3].get_text()),
                 'normal_number': int(tds[4].get_text()),
                 'total_area': tds[5].get_text(),
                 'normal_area': float(tds[6].get_text().split(' ')[0]),
                 'status': tds[7].get_text()
                 })
        if not presale_list:
            raise Exception('parse error.')
        
        return presale_list
            
        

class House(BaseMixin, Base):
    __tablename__ = 'house'
    
    def __init__(self, *args, **kwargs):
        Base.__init__(self, *args, **kwargs)
        if not self.last_track_week:
            self.last_track_week = week_number()
    
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
    
    last_track_week = Column(INTEGER)
  
    community = relationship('Community', back_populates='houses')
    

class CommunityRecord(BaseMixin, Base):
    __tablename__ = 'community_record'
    
    def __init__(self, *args, **kwargs):
        Base.__init__(self, *args, **kwargs)
        if not self.create_week:
            self.create_week = week_number()
    
    community_id = Column(INTEGER, ForeignKey('community.id'), nullable=False)
    
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
    
    house_download_finish = Column(BOOLEAN, default=False)
    house_parse_finish = Column(BOOLEAN, default=False)
    create_week = Column(INTEGER, nullable=False)
    
    community = relationship('Community', back_populates='community_records')
    
Community.houses = relationship('House', order_by=House.view_last_month,
                                back_populates='community')
Community.community_records = relationship('CommunityRecord', 
                                           order_by=CommunityRecord.view_last_month,
                                           back_populates='community')


class HouseRecord(BaseMixin, Base):
    __tablename__ = 'house_record'
    
    def __init__(self, *args, **kwargs):
        Base.__init__(self, *args, **kwargs)
        if not self.create_week:
            self.create_week = week_number()
    
    community_id = Column(INTEGER, ForeignKey('community.id'), nullable=False)
    house_id = Column(INTEGER, ForeignKey('house.id'), nullable=False)
    
    price = Column(INTEGER)
    price_change = Column(INTEGER)
    view_last_month = Column(INTEGER)
    view_last_week = Column(INTEGER)
    
    create_week = Column(INTEGER, nullable=False)
    
    community = relationship('Community', back_populates='house_records')
    
class PresalePermit(BaseMixin, Base):
    __tablename__ = 'presale_permit'
    
    community_id = Column(INTEGER, ForeignKey('community.id'), nullable=False)
    
    serial_number = Column(VARCHAR(32))
    description = Column(VARCHAR(1024))
    sale_date = Column(DATE)
    total_number = Column(INTEGER)
    normal_number = Column(INTEGER)
    total_area = Column(FLOAT)
    normal_area = Column(FLOAT)
    status = Column(VARCHAR(64))
    
class Job(BaseMixin, Base):
    __tablename__ = 'job'
    
    batch_number = Column(INTEGER, nullable=False)
    status = Column(VARCHAR(64), default='ready')
    target_url = Column(VARCHAR(1024), nullable=False)
    parameters = Column(PickleType, default={})
    type = Column(VARCHAR(64))
    
    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'job',
        }
    
class DistrictJob(Job):
    district_id = Column(INTEGER, ForeignKey('district.id'))
    district = relationship('District', backref=backref('jobs'),
                            foreign_keys=district_id)
    
    __mapper_args__ = {
        'polymorphic_identity': 'district_job',
        }
        
    def start(self, session):
        page_index = self.parameters['page']
        res = http_request(self.target_url, 'get')
        if res.status_code != 200:
            logger.error('bad response: %s, %s', res.status_code, 
                         self.target_url)
            raise Exception
        res.encoding = 'gb2312'
        community_list, total_page = self.district.parse_html(res.text, 
                                                              page_index)
        
        if page_index == 1:
            # add district job for the rest pages
            for i in range(total_page-1):
                session.add(DistrictJob(district=self.district, 
                                        batch_number=self.batch_number,
                                        target_url=self.district.fd_search_url(i+2),
                                        parameters={'page': i+2}))
        
        # add community job
        for c_info in community_list:
            community = (session.query(CommunityFD)
                                .filter_by(outer_id=c_info['outer_id'])
                                .first())
            if not community:
                community = CommunityFD(district=self.district,
                                        **c_info)
                session.add(community)
            session.add(CommunityJob(batch_number=self.batch_number,
                                     target_url=community.presale_url(),
                                     community=community))
        self.status = 'succeed'    
        session.commit()
            

class CommunityJob(Job):
    community_id = Column(INTEGER, ForeignKey('community.id'))
    community = relationship('Community', backref=backref('jobs'),
                             foreign_keys=community_id)
    __mapper_args__ = {
        'polymorphic_identity': 'community_job',
        }
    
    def start(self, session):
        res = http_request(self.target_url, 'get')
        if res.status_code != 200:
            logger.error('bad response: %s, %s', res.status_code, 
                         self.target_url)
            raise Exception
        res.encoding = 'gb2312'
        presales = self.community.parse_presale_page(res.text)
        
        for presale_info in presales:
            presale = (session.query(PresalePermit)
                              .filter_by(community_id=self.community.id,
                                         serial_number=presale_info['serial_number'])
                              .first())
            if not presale:
                presale_info['community_id'] = self.community.id
                session.add(PresalePermit(**presale_info))
        self.status='succeed'
        
        session.commit()

Community.house_records = relationship('HouseRecord', 
                                       order_by=HouseRecord.create_week,
                                       back_populates='community')

    
def week_number():
    import house_tracker_settings as settings
    day_number = (date.today() - settings.original_date).days
    return int(math.ceil(day_number / 7.0))


def http_request(url, method, **kwargs):
    try_times = 0
    
    while True:
        try:
            try_times += 1
            return getattr(requests, method)(url, **kwargs)
        except requests.exceptions.RequestException as e:
            logger.exception(e)
            if try_times <3:
                logger.warn('%s-th try failed: %s.', try_times, url)
                time.sleep(3)
            else:
                break
    logger.error('get html failed: %s', url)
    raise Exception('http request error: %s', url)

    
