
import math
from datetime import date, datetime

from sqlalchemy import (Column, Integer, String, DateTime, Boolean, Float, 
                        ForeignKey)

from common.db import Base, BaseMixin


class Community(BaseMixin, Base):
    
    outer_id = Column('outer_id', String(128), nullable=False)
    name = Column('name', String(64), nullable=False)
    district = Column('district', String(32), nullable=False)
    area = Column('area', String(32))
    
    average_price = Column('average_price', Integer)
    house_available = Column('house_available', Integer)
    sold_last_season = Column('sold_last_season', Integer)
    view_last_month = Column('view_last_month', Integer)
    
    last_track_week = Column('last_track_week', Integer)
  

class House(BaseMixin, Base):
    
    def __init__(self, *args, **kwargs):
        Base.__init__(self, *args, **kwargs)
        if not self.last_track_week:
            self.last_track_week = week_number()
    
    community_id = Column('community_id', None, ForeignKey('community.id'),
                          nullable=False)
    outer_id = Column('outer_id', String(128), nullable=False)
    
    area = Column('area', Float)
    room = Column('room', String(64))
    build_year = Column('build_year', Integer)
    floor = Column('floor', String(64))
    available = Column('available', Boolean, default=True)
    
    price_origin = Column('price_origin', Integer, nullable=False)
    price = Column('price', Integer, nullable=False)
    view_last_month = Column('view_last_month', Integer)
    view_last_week = Column('view_last_week', Integer)
    new = Column('new', Boolean, default=True)
    available_change_times = Column('available_change_times', Integer, 
                                    default=0)
    
    last_track_week = Column('last_track_week', Integer)
    

class CommunityRecord(BaseMixin, Base):
    
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
    
    rise_number = Column('rise_number', Integer)
    reduce_number = Column('reduce_number', Integer)
    valid_unchange_number = Column('valid_unchange_number', Integer)
    new_number = Column('new_number', Integer)
    miss_number = Column('miss_number', Integer)
    view_last_week = Column('view_last_week', Integer)
    
    house_download_finish = Column('house_download_finish', Boolean, 
                                   default=False)
    house_parse_finish = Column('house_parse_finish', Boolean, default=False)
    create_week = Column('create_week', Integer, nullable=False)


class HouseRecord(BaseMixin, Base):
    
    def __init__(self, *args, **kwargs):
        Base.__init__(self, *args, **kwargs)
        if not self.create_week:
            self.create_week = week_number()
    
    id = Column('id', Integer, primary_key=True, autoincrement=True)
    community_id = Column('community_id', None, ForeignKey('community.id'),
                          nullable=False)
    house_id = Column('house_id', None, ForeignKey('house.id'), nullable=False)
    
    price = Column('price', Integer)
    price_change = Column('price_change', Integer)
    view_last_month = Column('view_last_month', Integer)
    view_last_week = Column('view_last_week', Integer)
    
    create_week = Column('create_week', Integer, nullable=False)

    
def week_number():
    import settings
    day_number = (date.today() - settings.original_date).days
    return int(math.ceil(day_number / 7.0))


    