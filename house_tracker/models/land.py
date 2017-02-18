# coding=utf-8

import re
import json

from bs4 import BeautifulSoup
from sqlalchemy import Column, ForeignKey
from sqlalchemy.dialects.mysql import VARCHAR, INTEGER, FLOAT, DATE, TEXT
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import DeclarativeMeta

from .base import BaseMixin, Base, District


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

        data = json.loads(tag_text[left: right + 1].strip())['data'][0]
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

__all__ = ['LandSoldRecord', 'Land', 'LandResidential', 'LandSemiResidential',
           'LandRelocation']
