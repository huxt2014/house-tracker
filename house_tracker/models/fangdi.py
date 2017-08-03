# coding=utf-8

import re
import codecs
import random
import logging
import urllib.parse
from datetime import date, datetime

from bs4 import BeautifulSoup
from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, INTEGER, BOOLEAN, FLOAT, DATE
from sqlalchemy.orm import relationship, backref, joinedload
from requests import Request

from . import base
from .base import (District, Community, IdMixin, Base, BatchJob, Job,
                   PagesIterator)
from .. import utils
from ..exceptions import JobError, ParseError, DownloadError


logger = logging.getLogger(__name__)
ENCODING = "gbk"


class DistrictFD(District):

    # jobs = relationship('DistrictJob')

    SEARCH_URL = "http://www.fangdi.com.cn/complexpro.asp"

    def fd_search(self, http_session, page):
        req = self._fd_search_request(page)
        resp = utils.do_http_request(http_session, req, timeout=10)
        resp.encoding = ENCODING
        return self._fd_parse(resp, page)

    def _fd_search_request(self, page):
        params = {"page": page,
                  "districtID": self.outer_id,
                  "Region_ID": "",
                  "projectAdr": "",
                  "projectName": "",
                  "startCod": "",
                  "buildingType": 1,
                  "houseArea": 0,
                  "averagePrice": 0,
                  "selState": "",
                  "selCircle": 0}
        return Request(url=DistrictFD.SEARCH_URL, method="GET", params=params)

    def _fd_parse(self, resp, page):
        """The html page has the following skeleton:
        <HTML>...<body>
            ...
            <table>
                <tr>
                    <td>状态</td>
                    <td>项目名称</td>
                    <td>所在区县</td>
                    ...
                </tr>
                ...
                <tr valign="middle">
                    <td>在售</td>
                    <td><a href=proDetail.asp?projectID=ODYyOXwyMDE3LTYtMjJ8NjM=>嘉誉都汇广场</a></td>
                    <td>殷行路1280号等</td>
                    <td>1093</td>
                    <td>81012.31</td>
                    <td>杨浦区</td>
                </tr>
                ...
                <table>
                    <tr><td><td>第1页/共16页</td>
                    ...
                </table>
            ...
            </table>
        </body></html>
        """
        soup = BeautifulSoup(resp.text, 'html.parser')
        community_list = []

        for table in soup.find_all('table'):
            target_cols = table.tr.find_all('td', string=['项目地址', '所在区县'],
                                            recursive=False)
            if len(target_cols) == 2:
                break
        else:
            raise ParseError("can not find target content of fangdi.com.cn"
                             " district page: %s" % resp.url)

        # parse community row
        for row in table.find_all('tr'):
            if not row.get('valign'):
                continue
            tds = row.find_all('td', recursive=False)

            if self.name != tds[5].string:
                raise ParseError("request fangdi.com.cn district page with name"
                                 " %s, but get page with name %s: %s" %
                                 (self.name, tds[5].string, resp.url))

            href = urllib.parse.urlparse(tds[1].a["href"])
            project_id = urllib.parse.parse_qs(href.query)["projectID"][0]
            outer_id = CommunityFD.fd_decode_project_id(project_id)

            c_info = {'outer_id': outer_id,
                      'name': tds[1].get_text(),
                      'location': tds[2].string,
                      'total_number': int(tds[3].string),
                      'total_area': float(tds[4].string)}
            community_list.append(c_info)

        # parse page number
        sub_table = table.table
        result = re.search("第(\d+)页/共(\d+)页",
                           sub_table.tr.td.td.get_text())
        current_page = int(result.group(1))
        total_page = int(result.group(2))
        if current_page != page:
            raise ParseError("request fangdi.com.cn district page of page %s,"
                             " but get page %s: %s"
                             % (page, current_page, resp.url))

        if not community_list:
            raise ParseError("no community found in fangdi.com.cn district"
                             " page: %s" % resp.url)

        return {"community_list": community_list,
                "total_page": total_page}

    def __str__(self):
        return '%s-%s' % (self.id, self.name)


class CommunityFD(Community):
    total_number = Column(INTEGER)
    total_area = Column(FLOAT)
    location = Column(VARCHAR(1024))
    company = Column(VARCHAR(1024))
    track_presale = Column(BOOLEAN)
    presale_url_name = Column(VARCHAR(1024))
    # presales = relationship('PresalePermit')

    district = relationship(DistrictFD, foreign_keys=Community.district_id)

    __mapper_args__ = {
        'polymorphic_identity': 'fangdi',
    }

    PRESEIL_URL = "http://www.fangdi.com.cn/Presell.asp"
    COMMUNITY_URL = "http://www.fangdi.com.cn/proDetail.asp"

    def __init__(self, name, outer_id, district, **kwargs):
        Community.__init__(self, name, outer_id, district)
        for key in ('total_number', 'total_area', 'location', 'company'):
            if key in kwargs.keys():
                setattr(self, key, kwargs[key])
        self.track_presale = True

    def load_detail(self, http_session):
        resp = utils.do_http_request(http_session, self._fd_community_request(),
                                     timeout=10)
        resp.encoding = ENCODING
        c_info = self._fd_parse_community(resp)
        self.company = c_info["company"]
        self.presale_url_name = c_info["presale_url_name"]

    def fd_search_presale(self, http_session):
        resp = utils.do_http_request(http_session, self._fd_presale_request(),
                                     timeout=10)
        resp.encoding = ENCODING
        return self._fd_parse_presale(resp)

    def check_presale_permit(self, db_session, http_session):

        if self.presale_url_name is None:
            self.load_detail(http_session)

        presales = self.fd_search_presale(http_session)
        old_presales = set(p.serial_number for p in self.presales)

        for presale_info in presales:
            if not presale_info['serial_number'] in old_presales:
                db_session.add(PresalePermit(self, **presale_info))

    @staticmethod
    def fd_decode_project_id(content):
        return codecs.decode(bytes(content, "ascii"), "base64"
                             ).decode('ascii').split('|')[0]

    def _fd_presale_request(self):
        if self.presale_url_name is None:
            raise DownloadError("presale_url_name not exist: community id = %s"
                                % self.id)
        project_name = self.presale_url_name.encode(ENCODING)
        params = {"projectID": self._tmp_id,
                  "projectname": project_name}
        return Request(url=CommunityFD.PRESEIL_URL, method="GET", params=params)

    def _fd_community_request(self):
        return Request(url=CommunityFD.COMMUNITY_URL, method="GET",
                       params={"projectID": self._tmp_id})

    @property
    def _tmp_id(self):
        today = date.today().isoformat().replace('-0', '-')
        tail = random.randint(1, 99)
        tmp_id = bytes('%s|%s|%s' % (self.outer_id, today, tail), 'ascii')
        return codecs.encode(tmp_id, "base64").strip().decode("ascii")

    def _fd_parse_presale(self, resp):
        """The html page has the following skeleton:
        <html>
        ...
        <table>
            <tr>
                <td>编号</td>
                <td>预售许可证/房地产权证</td>
                <td>开盘日期</td>
                <td>总套数</td>
                <td>住宅套数</td>
                <td>总面积</td>
                <td>住宅面积</td>
                <td>销售状态</td>
            </tr>
            <tr onclick=""> ...</tr>
            <tr onclick=""> ...</tr>
        </table>
        ...
        </html>
        """
        soup = BeautifulSoup(resp.text, 'html.parser')
        table = soup.table
        test_cols = table.tr.find_all('td', string=['开盘日期', '总套数'],
                                      recursive=False)
        if len(test_cols) != 2:
            raise ParseError("can not find target content of fangdi.com.cn"
                             " presale page: %s" % resp.url)

        presale_list = []
        for row in table.find_all('tr', recursive=False):
            if not row.get('onclick'):
                continue
            tds = row.find_all('td', recursive=False)

            serial_number = tds[0].get_text()

            date_str = tds[2].get_text()
            try:
                sale_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            except ValueError:
                # date may be null
                logger.warning("parse presale date of serial number %s failed,"
                               " community_id=%s: %s",
                               serial_number, self.id, date_str)
                sale_date = date.today()

            try:
                status = tds[7].get_text()
            except Exception as e:
                # status may be null
                logger.warning("get presale status of community %s failed: %s",
                               self.id, e.__str__())
                status = None

            presale_list.append(
                {'serial_number': serial_number,
                 'description': tds[1].get_text(),
                 'sale_date': sale_date,
                 'total_number': int(tds[3].get_text()),
                 'normal_number': int(tds[4].get_text()),
                 'total_area': tds[5].get_text(),
                 'normal_area': float(tds[6].get_text().split(' ')[0]),
                 'status': status
                 })

        if not presale_list:
            raise ParseError("presale list not found in fangdi.com.cn presale"
                             " page: %s" % resp.url)

        return presale_list

    def _fd_parse_community(self, resp):
        """The html page has the following skeleton:
        <html>
        ...
        ...
            <table>
                <tr>
                    <td>项目名称：</td>
                    ...
                </tr>
                <tr>
                    <td>项目地址：</td>
                    <td>殷行路1280号等</td>
                    <td>所属板块：</td>
                    <td>新江湾城板块</td>
                </tr>
                <tr>
                    <td>企业名称：</td>
                    <td>上海城投悦城置业有限公司</td>
                    ...
                </tr>
        ...
        ...
            <iframe src='Presell.asp?projectID=ODYyOXwyMDE3LTYtMjJ8NjM=&projectname=嘉誉都汇广场'>
            </iframe>
        ...
        </html>
        """
        soup = BeautifulSoup(resp.text, 'html.parser')

        for table in soup.find_all('table'):
            # search recursively
            try:
                if table.tr.td.get_text() == '项目名称：':
                    break
            except AttributeError:
                continue
        else:
            raise ParseError("can not find target content in fangdi.com.cn"
                             " community page: %s" % resp.url)

        trs = table.find_all('tr', recursive=False)

        # get area
        tds = trs[1].find_all('td', recursive=False)
        area_name = tds[3].get_text()

        # get company
        tds = trs[2].find_all('td', recursive=False)
        company = tds[1].get_text()

        # get community name for presale
        src = urllib.parse.urlparse(soup.iframe['src'])
        params = urllib.parse.parse_qs(src.query)
        project_id = self.fd_decode_project_id(params["projectID"][0])
        if self.outer_id != project_id:
            raise ParseError("request fangdi.com.cn community page with "
                             "outer_id %s, but get outer_id %s: %s"
                             % (self.outer_id, project_id, resp.url))
        presale_url_name = params["projectname"][0]

        return {"area_name": area_name,
                "company": company,
                "presale_url_name": presale_url_name}

    def __str__(self):
        return '%s, %s' % (self.district.name, self.name)


class PresalePermit(IdMixin, Base):
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

    def __init__(self, community, **kwargs):
        Base.__init__(self, community=community, **kwargs)


class BatchJobFD(BatchJob):
    __mapper_args__ = {
        'polymorphic_identity': b"fangdi" + bytes(2),
    }

    def _start(self, district_ids=None):

        result = base.FINISHED

        for district, job in self.get_district_and_job(district_ids):
            if job is not None and job.status == base.FINISHED:
                continue
            elif job is None:
                job = DistrictJob(district, self)
                self.db_session.add(job)
                self.commit()

            try:
                job.start(self.db_session, self.http_session,
                          auto_commit=self.auto_commit)
            except JobError as e:
                logger.error("DistrictJob of id %s failed: %s", job.id, e)
                job.status = base.FAILED
                self.commit()
                result = base.FAILED

        return result

    def get_district_and_job(self, district_ids=None):
        if district_ids is not None:
            filter_ = DistrictFD.id.in_(district_ids)
        else:
            filter_ = None

        return self._get_obj_and_job(DistrictFD, DistrictJob,
                                     DistrictJob.district_id == DistrictFD.id,
                                     filter_=filter_,
                                     order=DistrictFD.outer_id)

    def mail_content(self):

        if self.status != base.FINISHED or self.db_session is None:
            return BatchJob.mail_content(self)

        content = "%s \r\n" % BatchJob.mail_content(self)

        content += "新出现的小区:\r\n"
        new_cs = {}
        rs = (self.db_session.query(CommunityFD)
              .filter(CommunityFD.created_at.between(self.created_at,
                                                     self.last_modified_at))
              .all())
        for c in rs:
            new_cs[c.id] = c

        if not new_cs:
            content += "    无 \r\n"
        else:
            for c in new_cs.values():
                line = "    %s, %s \r\n" % (
                       c.name, c._fd_community_request().prepare().url)
                content += line

        content += "新发预售证的小区:\r\n"
        rs = (self.db_session.query(PresalePermit)
              .options(joinedload("community"))
              .filter(PresalePermit.created_at.between(self.created_at,
                                                       self.last_modified_at))
              .all())

        new_ps = []
        for p in rs:
            if p.community.id not in new_cs:
                new_ps.append(p)
                new_cs[p.community.id] = p.community

        if not new_ps:
            content += "    无 \r\n"
        else:
            for p in new_ps:
                c = p.community
                line = "    %s, %s \r\n" % (
                    c.name, c._fd_community_request().prepare().url)
                content += line

        if new_cs or new_ps:
            content += "注：链接当日内有效"

        return content


class DistrictJob(Job):
    district_id = Column(INTEGER)
    district = relationship(DistrictFD, foreign_keys=district_id,
                            primaryjoin=district_id==DistrictFD.id)

    __mapper_args__ = {
        'polymorphic_identity': 'district_job',
    }

    def __init__(self, district, batch_job):
        Job.__init__(self, batch_job)
        self.district = district
        self.district_id = district.id

    def _start(self):

        result = base.FINISHED

        existing_outer_ids = {}
        skip_outer_ids = set()
        query = (self.db_session.query(CommunityFD)
                 .filter_by(district_id=self.district.id))
        for c in query.all():
            existing_outer_ids[c.outer_id] = c
            if not c.track_presale:
                skip_outer_ids.add(c.outer_id)

        # parse each page
        for content in PagesIterator(self, self.district.fd_search):
            c_list = content["community_list"]
            for c_info in c_list:
                outer_id = c_info.pop('outer_id')
                if outer_id not in existing_outer_ids:
                    c = CommunityFD(c_info.pop('name'), outer_id, self.district,
                                    **c_info)
                    self.db_session.add(c)
                    self.db_session.flush()
                    existing_outer_ids[outer_id] = c
                elif outer_id not in skip_outer_ids:
                    c = existing_outer_ids[outer_id]
                else:
                    c = None

                if c is not None:
                    try:
                        c.check_presale_permit(self.db_session,
                                               self.http_session)
                    except (DownloadError, ParseError) as e:
                        self.db_session.rollback()
                        raise JobError("%s: %s" % (e.__class__.__name__, e))

            self.commit()

        return result


__all__ = ['DistrictFD', 'CommunityFD', 'PresalePermit', 'BatchJobFD',
           'DistrictJob']
