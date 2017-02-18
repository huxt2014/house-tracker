# coding=utf-8

import re
import urllib
import random
import logging
from datetime import date

from bs4 import BeautifulSoup
from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, INTEGER, BOOLEAN, FLOAT, DATE
from sqlalchemy.orm import relationship, backref

from .base import (District, Community, BaseMixin, Base, BatchJob, Job,
                   JobWithCommunity)
from ..exceptions import BatchJobError, JobError


logger = logging.getLogger(__name__)


class DistrictFD(District):
    __tablename__ = District.__tablename__
    __table_args__ = {
        'extend_existing': True
    }

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

    def __str__(self):
        return '%s-%s' % (self.id, self.name)


class CommunityFD(Community):
    total_number = Column(INTEGER)
    total_area = Column(FLOAT)
    location = Column(VARCHAR(1024))
    track_presale = Column(BOOLEAN)
    presale_url_name = Column(VARCHAR(1024))
    company = Column(VARCHAR(1024))
    # presales = relationship('PresalePermit')

    district = relationship(DistrictFD, foreign_keys=Community.district_id)

    __mapper_args__ = {
        'polymorphic_identity': 'fangdi',
    }

    def __init__(self, name, outer_id, district, **kwargs):
        Community.__init__(self, name, outer_id, district)
        for key in ('total_number', 'total_area', 'location', 'company'):
            if key in kwargs.keys():
                setattr(self, key, kwargs[key])
        self.track_presale = True

    def presale_url(self):
        project_name = (self.presale_url_name or self.name).encode('gbk')
        query = urllib.urlencode({'projectname': project_name})
        return 'http://www.fangdi.com.cn/Presell.asp?projectID=%s&%s' % (
            self.tmp_id, query)

    def community_url(self):
        return 'http://www.fangdi.com.cn/proDetail.asp?projectID=%s' % self.tmp_id

    @property
    def tmp_id(self):
        today = date.today().isoformat().replace('-0', '-')
        tail = random.randint(1, 99)
        tmp_id = ('%s|%s|%s' % (self.outer_id, today, tail))
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
                sale_date = date(
                    *(int(e) for e in tds[2].get_text().split('-')))
            except Exception as e:
                # date may be null
                logger.warn('%s get date failed: %s', self.id, e.__str__())
                sale_date = date.today()

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

    def __str__(self):
        return '%s, %s' % (self.district.name, self.name)


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


class BatchJobFD(BatchJob):
    __mapper_args__ = {
        'polymorphic_identity': 'fangdi',
    }

    def _initial(self, session):
        for d in session.query(DistrictFD).all():
            job = DistrictJob(d, self, 1)
            session.add(job)
            first_page = job.get_web_page(cache=True)
            cs, total_page = job.district.parse_fd_html(first_page, 1)
            for i in range(total_page - 1):
                session.add(DistrictJob(d, self, i + 2))
            logger.info('district job init finish: %s', d)

    def _start(self, session):
        c_set = (session.query(CommunityFD.outer_id, CommunityFD.track_presale)
                 .all())
        notrack_outer_ids = set(c.outer_id for c in c_set
                                if not c.track_presale)

        for job_cls in (DistrictJob, CommunityJobFD, PresaleJob):
            logger.info('%s begin...', job_cls)
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


class DistrictJob(Job):
    district_id = Column(INTEGER, ForeignKey('district.id'))
    district = relationship(DistrictFD, foreign_keys=district_id)

    __mapper_args__ = {
        'polymorphic_identity': 'district_job',
    }

    def __init__(self, district, batch_job, page):
        Job.__init__(self, batch_job, parameters={'page': page})
        self.district = district
        self.district_id = district.id

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
                community = CommunityFD(c_info.pop('name'),
                                        c_info.pop('outer_id'), self.district,
                                        **c_info)
                session.add(community)
                session.add(CommunityJobFD(community, self.batch_job))
                logger.info('new community: %s', community)

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


class CommunityJobFD(JobWithCommunity):
    __mapper_args__ = {
        'polymorphic_identity': 'community_job_fangdi',
    }

    def inner_start(self, session, **kwargs):
        content = self.get_web_page()
        c_info = self.community.parse_community_page(content)

        for key, value in c_info.iteritems():
            setattr(self.community, key, value)

    def web_uri(self):
        return self.community.community_url()

    def web_encode(self):
        return 'gbk'


class PresaleJob(JobWithCommunity):
    __mapper_args__ = {
        'polymorphic_identity': 'presale_job',
    }

    def inner_start(self, session, **kwargs):
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

__all__ = ['DistrictFD', 'CommunityFD', 'PresalePermit', 'BatchJobFD',
           'DistrictJob', 'CommunityFD', 'PresaleJob']
