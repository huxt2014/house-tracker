
from datetime import datetime

from sqlalchemy import Column, Integer, String, Date, Boolean, Float,ForeignKey
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Community(Base):
    __tablename__ = 'communities'
    
    id = Column('id', Integer, primary_key=True, autoincrement=True)
    outer_id = Column('outer_id', String(128), nullable=False)
    name = Column('name', String(64), nullable=False)
    district = Column('district', String(32), nullable=False)
    area = Column('area', String(32))
    
    average_price = Column('average_price', Integer)
    house_available = Column('house_available', Integer)
    sold_last_season = Column('sold_last_season', Integer)
    view_last_month = Column('view_last_month', Integer)
    
    last_track_week = Column('last_track_week', Integer)
    date_create = Column('date_create', Date, nullable=False)
    
    def __repr__(self):
        return ("%s-%s-%s" % (self.id, self.outer_id, self.name))
    

class House(Base):
    __tablename__ = 'houses'
    
    id = Column('id', Integer, primary_key=True, autoincrement=True)
    community_id = Column('community_id', None, ForeignKey('communities.id'),
                          nullable=False)
    outer_id = Column('outer_id', String(128), nullable=False)
    
    area = Column('area', Float)
    room = Column('room', String(64))
    build_year = Column('build_year', Integer)
    floor = Column('floor', String(64))
    available = Column('available', Boolean)
    
    price = Column('price', Integer)
    view_last_month = Column('view_last_month', Integer)
    view_last_week = Column('view_last_week', Integer)
    
    last_track_week = Column('last_track_week', Integer)
    create_date = Column('create_date', Date, default=datetime.now().date)
    

class CommunityRecord(Base):
    __tablename__ = 'community_records'
    
    id = Column('id', Integer, primary_key=True, autoincrement=True)
    community_id = Column('community_id', None, ForeignKey('communities.id'),
                          nullable=False)
    
    average_price = Column('average_price', Integer)
    house_available = Column('house_available', Integer)
    sold_last_season = Column('sold_last_season', Integer)
    view_last_month = Column('view_last_month', Integer)
    
    house_download_finish = Column('house_download_finish', Boolean, 
                                   default=False)
    house_parse_finish = Column('house_parse_finish', Boolean, default=False)
    create_week = Column('create_week', Integer)
    create_date = Column('create_date', Date, default=datetime.now().date)
    

class HouseRecord(Base):
    __tablename__ = 'house_records'
    
    id = Column('id', Integer, primary_key=True, autoincrement=True)
    house_id = Column('house_id', None, ForeignKey('houses.id'), nullable=False)
    
    price = Column('price', Integer)
    view_last_month = Column('view_last_month', Integer)
    view_last_week = Column('view_last_week', Integer)
    
    download_finish = Column('download_finish', Boolean, default=False)
    parse_finish = Column('parse_finish', Boolean, default=False)
    create_week = Column('create_week', Integer)
    create_date = Column('create_date', Date, default=datetime.now().date)
    
    