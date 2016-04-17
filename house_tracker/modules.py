
import math
from datetime import date, datetime

from sqlalchemy import (Column, Integer, String, DateTime, Boolean, Float, 
                        ForeignKey)
from sqlalchemy.ext.declarative import declarative_base

from house_tracker.utils.conf_tool import GlobalConfig


class Base(object):
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column( DateTime, default=datetime.now)
    last_modified_at = Column( DateTime, onupdate=datetime.now)

    def __str__(self):
        content = 'id->%s ' % self.id
        attrs = [key for key in dir(self) 
                 if not key.startswith('_') and key != 'id' 
                    and key != 'metadata']
        for attr in attrs:
            content += '%s->%s ' % (attr, getattr(self, attr))
        return content

    
Base = declarative_base(cls=Base)

class Community(Base):
    __tablename__ = 'community'
    
    outer_id = Column('outer_id', String(128), nullable=False)
    name = Column('name', String(64), nullable=False)
    district = Column('district', String(32), nullable=False)
    area = Column('area', String(32))
    
    average_price = Column('average_price', Integer)
    house_available = Column('house_available', Integer)
    sold_last_season = Column('sold_last_season', Integer)
    view_last_month = Column('view_last_month', Integer)
    
    last_track_week = Column('last_track_week', Integer)
  

class House(Base):
    __tablename__ = 'house'
    
    community_id = Column('community_id', None, ForeignKey('community.id'),
                          nullable=False)
    outer_id = Column('outer_id', String(128), nullable=False)
    
    area = Column('area', Float)
    room = Column('room', String(64))
    build_year = Column('build_year', Integer)
    floor = Column('floor', String(64))
    available = Column('available', Boolean, default=True)
    
    price = Column('price', Integer)
    view_last_month = Column('view_last_month', Integer)
    view_last_week = Column('view_last_week', Integer)
    

class CommunityRecord(Base):
    __tablename__ = 'community_record'
    
    def __init__(self, *args, **kwargs):
        Base.__init__(self, *args, **kwargs)
        if not self.create_week:
            self.create_week = week_number()
    
    community_id = Column('community_id', None, ForeignKey('community.id'),
                          nullable=False)
    
    average_price = Column('average_price', Integer)
    house_available = Column('house_available', Integer)
    sold_last_season = Column('sold_last_season', Integer)
    view_last_month = Column('view_last_month', Integer)
    
    house_download_finish = Column('house_download_finish', Boolean, 
                                   default=False)
    house_parse_finish = Column('house_parse_finish', Boolean, default=False)
    create_week = Column('create_week', Integer, nullable=False)


class HouseRecord(Base):
    __tablename__ = 'house_record'
    
    def __init__(self, *args, **kwargs):
        Base.__init__(self, *args, **kwargs)
        if not self.create_week:
            self.create_week = week_number()
    
    id = Column('id', Integer, primary_key=True, autoincrement=True)
    community_id = Column('community_id', None, ForeignKey('community.id'),
                          nullable=False)
    house_id = Column('house_id', None, ForeignKey('house.id'), nullable=False)
    
    price = Column('price', Integer)
    view_last_month = Column('view_last_month', Integer)
    view_last_week = Column('view_last_week', Integer)
    
    create_week = Column('create_week', Integer, nullable=False)

    
def week_number():
    day_number = (date.today() - GlobalConfig().original_date).days
    return int(math.ceil(day_number / 7.0))


    