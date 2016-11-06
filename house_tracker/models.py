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
from sqlalchemy.orm import relationship, backref
from sqlalchemy.ext.declarative import declarative_base, declared_attr 
from sqlalchemy.orm.collections import InstrumentedList


__all__ = ['District', 'Community', 'CommunityLJ', 'CommunityFD', 'HouseLJ',
           'CommunityRecordLJ', 'HouseRecordLJ', 'PresalePermit', 'Job', 
           'DistrictJob', 'CommunityJobFD', 'CommunityJobLJ', 'PresaleJob',
           'HouseJobLJ', 'Area']

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
                
    def init_batch_jobs(self, session, batch_number, **kwargs):
        job = DistrictJob(self, 1, batch_number)
        session.add(job)
        content = job.get_web_page(cached=True)
        community_list, total_page = job.district.parse_html(content, 1)
        # add district job for the rest pages
        for i in range(total_page-1):
            session.add(DistrictJob(self, i+2, batch_number))
        job.start(session, auto_commit=False)
                   
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
    area_tmp = Column('area',VARCHAR(32))
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
                except ValueError:
                    if average_price == u'暂无均价':
                        c_info['average_price'] = None
                    else:
                        msg = ('parse community average price failed: '
                                '%s->%s.') % (community.id, community.outer_id)
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
                logger.warn('%s get date failed: %s', self.id, e.__str__())
                sale_date=None
                
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
            raise Exception('parse error')
        
        c_info = {}
        trs = table.find_all('tr', recursive=False)
        
        # get area
        tds = trs[1].find_all('td', recursive=False)
        c_info['area'] = tds[3].get_text()
        
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
    
    def __init__(self, community, batch_number, **kwargs):
        Base.__init__(self, community=community, batch_number=batch_number)
        for key, value in kwargs.iteritems():
            if hasattr(self, key):
                setattr(self, key, value)
    
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
    valid_average_price = Column(INTEGER)
    average_price_change = Column(INTEGER)
    
    batch_number = Column(INTEGER, nullable=False)
    
    community = relationship('Community', 
                             backref=backref('community_records',
                                             order_by=view_last_month))
    
    house_records = relationship('HouseRecordLJ',
                                 primaryjoin='(CommunityRecordLJ.community_id==foreign(HouseRecordLJ.community_id)'
                                             ')&(CommunityRecordLJ.batch_number==HouseRecordLJ.batch_number)',
                                 )
    
class HouseRecordLJ(BaseMixin, Base):
    __tablename__ = 'house_record'
    
    community_id = Column(INTEGER, ForeignKey('community.id'), nullable=False)
    house_id = Column(INTEGER, ForeignKey('house.id'), nullable=False)
    
    price = Column(INTEGER)
    price_change = Column(INTEGER)
    view_last_month = Column(INTEGER)
    view_last_week = Column(INTEGER)
    
    batch_number = Column(INTEGER, nullable=False)
    
    community = relationship('Community', foreign_keys=community_id,
                             backref=backref('house_records',
                                             order_by=batch_number))
    house = relationship('HouseLJ', backref=backref('house_records'),
                         foreign_keys=house_id)
    
    def __init__(self, house, batch_number, **kwargs):
        Base.__init__(self, house=house, community=house.community,
                      batch_number=batch_number)
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
    
class Job(BaseMixin, Base):
    __tablename__ = 'job'
    
    batch_number = Column(INTEGER, nullable=False)
    status = Column(VARCHAR(64), default='ready')
    target_uri = Column(VARCHAR(1024))
    parameters = Column(PickleType, default={})
    type = Column(VARCHAR(64))
    
    __mapper_args__ = {
        'polymorphic_on': type,
        'polymorphic_identity': 'job',
        }
    
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
            if interval_time:
                time.sleep(interval_time)
        
    def inner_start(self, session, **kwargs):
        """ Called by self.start. If any exception throw out,
        session.expunge_all will be called by self.start, and changes that 
        happen in inner_start on persistent objects and already flush or commit
        will not rollback. """
        
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
                res = requests.get(url)
            except requests.exceptions.RequestException as e:
                logger.exception(e)
                if try_times <3:
                    logger.warn('%s-th try failed: %s.', try_times, url)
                    time.sleep(3)
                else:
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
        
        return content
    
    def web_encode(self):
        return 'utf-8'
    
    def web_uri(self):
        raise NotImplementedError
    
    @property
    def cache_file_path(self):
        cache_dir = os.getenv('HOUSE_TRACKER_CACHE_DIR')
        if not hasattr(self, '_cache_file_path'):
            if not cache_dir or not hasattr(self, 'disk_uri'):
                self._cache_file_path = None
            else:
                self._cache_file_path = os.path.join(cache_dir,
                                                     self.disk_uri())        
        return self._cache_file_path
    
class DistrictJob(Job):

    district_id = Column(INTEGER, ForeignKey('district.id'))
    district = relationship('District', backref=backref('jobs'),
                            foreign_keys=district_id)
    __mapper_args__ = {
        'polymorphic_identity': 'district_job',
        }
    
    def __init__(self, district, page, batch_number):
        Job.__init__(self, district=district, batch_number=batch_number,
                     parameters={'page': 1})
        
    def inner_start(self, session, **kwargs):
        page_index = self.parameters['page']
        content = self.get_web_page()
        community_list, total_page = self.district.parse_html(content, 
                                                              page_index)
        # add community job
        for c_info in community_list:
            community = (session.query(CommunityFD)
                                .filter_by(outer_id=c_info['outer_id'])
                                .first())
            if not community:
                community = CommunityFD(district=self.district,
                                        track_presale=True,
                                        **c_info)
                session.add(community)
            
            if community.track_presale:
                session.add(PresaleJob(batch_number=self.batch_number,
                                       community=community))
        self.status = 'succeed'
    
    def web_uri(self):
        page_index = self.parameters['page']
        return self.district.fd_search_url(page_index)
    
    def web_encode(self):
        return 'gbk'
    
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
    
    def inner_start(self, session):    
        self.target_url = self.community.community_url()
        
        res = http_request(self.target_url, 'get')
        if res.status_code != 200:
            logger.error('bad response: %s, %s', res.status_code, 
                         self.target_url)
            raise Exception
        res.encoding = 'gbk'
        c_info = self.community.parse_community_page(res.text)
        
        for key, value in c_info.iteritems():
            setattr(self.community, key, value)
        self.status = 'succeed'
        
    
class CommunityJobLJ(CommunityJob):
    __mapper_args__ = {
        'polymorphic_identity': 'community_job_lianjia',
        }
    
    def __init__(self, community, page, batch_number):
        Job.__init__(self, community=community, batch_number=batch_number,
                     parameters={'page': page})
    
    def inner_start(self, session, **kwargs):
        page = self.parameters['page']
        
        content = self.get_web_page()
        c_info, house_ids = self.community.parse_page(content, page)
            
        if page == 1:
            c_record = CommunityRecordLJ(self.community, self.batch_number,
                                         **c_info)
            self.community.update(**c_info)
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
            session.add(HouseJobLJ(house, self.batch_number))
        
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
    
    def __init__(self, house, batch_number):
        Job.__init__(self, batch_number=batch_number, house=house,
                     community=house.community)
    
    def inner_start(self, session):
        house_info = self.house.parse_page(self.get_web_page())
        h_record = HouseRecordLJ(self.house, self.batch_number, **house_info)
        if self.house.new:
            for key in ('room', 'area', 'floor', 'build_year'):
                setattr(self.house, key, house_info[key])
            self.house.price_origin = house_info['price']
        else:
            h_record.price_change = h_record.price - self.house.price
        for key in ('price', 'view_last_week', 'view_last_month'):
            setattr(self.house, key, house_info[key])
        
        self.house.last_batch_number = self.batch_number
        self.house.available = True
        session.add(h_record)
        
    def web_uri(self):
        return self.house.download_url()
    
            
class PresaleJob(CommunityJobFD):
    
    __mapper_args__ = {
        'polymorphic_identity': 'presale_job',
        }
    
    def inner_start(self, session):
        self.target_url = self.community.presale_url()
        res = http_request(self.target_url, 'get')
        if res.status_code != 200:
            logger.error('bad response: %s, %s', res.status_code, 
                         self.target_url)
            raise Exception
        res.encoding = 'gbk'
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

    
