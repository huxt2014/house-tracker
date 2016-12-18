# coding=utf-8

import os
import sys
import argparse
import logging.config
from datetime import datetime, timedelta

from sqlalchemy import or_, and_
from sqlalchemy.orm import joinedload
from sqlalchemy.sql import func
from sqlalchemy.sql.expression import case
import alembic.config, alembic.util

import house_tracker.models as ht_models
from house_tracker.config import Config
from house_tracker.exceptions import ConfigError
from house_tracker.utils.db import get_database_url, get_session
from .workers import InitWorker, StartWorker

import house_tracker_settings as settings
logger = logging.getLogger(__name__)


class FactoryMeta(type):
    """
    """
    def __call__(cls, *args, **kwargs):
        if kwargs.get('argv'):
            cmd_args = FactoryMeta.parser.parse_args(kwargs.pop('argv'))
        else:
            cmd_args = FactoryMeta.parser.parse_args()
        
        instance = type.__call__(
                        FactoryMeta.subcommand_map[cmd_args.subcommand],
                        *args, cmd_args=cmd_args, config = Config(), **kwargs)
        return instance
    
    def __init__(cls, classname, superclasses, attributedict):
        type.__init__(cls, classname, superclasses, attributedict)
        if 'register_subcommand' in attributedict:
            choices = FactoryMeta.subparsers.choices
            len_tmp = len(choices)
            cls.register_subcommand(FactoryMeta.subparsers)
            if len(choices) - len_tmp != 1:
                raise Exception('register_subcommand should add an subcommand.')
            FactoryMeta.subcommand_map[choices.keys()[-1]] = cls
        
        if 'config_parser' in attributedict:
            cls.config_parser(FactoryMeta.parser)
    
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='subcommand')
    subcommand_map = {}


class Command():
    __metaclass__ = FactoryMeta
    
    @staticmethod
    def config_parser(parser):
        parser.add_argument('-d', '--debug', action='store_true')
    
    def __init__(self, cmd_args=None, config=None):
        self.cmd_args = cmd_args
        self.config=config
        if self.cmd_args.debug:
            logging.getLogger().setLevel(logging.DEBUG)

    def start(self):
        raise NotImplementedError


class Migrate(Command):
    @staticmethod
    def register_subcommand(subparsers):
        alembic_cmd = alembic.config.CommandLine()
        subparsers._name_parser_map['migrate'] = alembic_cmd.parser
    
    def start(self):
        parser = Command.subparsers.choices['migrate']
        if not hasattr(self.cmd_args, "cmd"):
            # see http://bugs.python.org/issue9253, argparse
            # behavior changed incompatibly in py3.3
            parser.error("too few arguments")
        else:
            alembic_cfg = alembic.config.Config()
            alembic_cfg.set_main_option("script_location", 
                                        "house_tracker:migrations")
            alembic_cfg.set_main_option("sqlalchemy.url", get_database_url())
            
            fn, positional, kwarg = self.cmd_args.cmd
            try:
                fn(alembic_cfg,
                   *[getattr(self.cmd_args, k) for k in positional],
                   **dict((k, getattr(self.cmd_args, k)) for k in kwarg)
                   )
            except alembic.util.CommandError as e:
                if self.cmd_args.raiseerr:
                    raise
                else:
                    alembic.util.err(str(e))

class BatchJobCommand(Command):
    def __init__(self, cmd_args=None, config=None):
        Command.__init__(self, cmd_args=cmd_args, config=config)
        
        self.batchjob_args = {'cache_dir':self.config.data_dir,
                              'interval_time': self.config.interval_time,
                              'clean_cache': self.cmd_args.clean_cache,
                              }

class InitBatchJob(BatchJobCommand):
    @staticmethod
    def register_subcommand(subparsers):
        subparser = subparsers.add_parser('init')
        subparser.add_argument('--clean-cache', action='store_true')
        
    def start(self):
        worker = InitWorker(ht_models.BatchJobFD, get_session(),
                            batchjob_args=self.batchjob_args)
        worker.start()
        worker.join()
        
    
class StartBatchJob(BatchJobCommand):
    @staticmethod
    def register_subcommand(subparsers):
        subparser = subparsers.add_parser('start')
        subparser.add_argument('--clean-cache', action='store_true')
    
    def start(self):
        worker = StartWorker(ht_models.BatchJobFD, get_session(),
                             batchjob_args=self.batchjob_args)
        worker.start()
        worker.join()

class RunServer(Command):
    @staticmethod
    def register_subcommand(subparsers):
        subparsers.add_parser('runserver')
    
    def __init__(self):
        print 'runserver'
        
class Dump(Command):
    @staticmethod
    def register_subcommand(subparsers):
        subparser = subparsers.add_parser('dump')
        subparser.add_argument('--fangdi', action='store_true')
        subparser.add_argument('-t', '--target', action='store', nargs='?')
        
    def start(self):
        session = get_session()
        path = self.cmd_args.target or '.'
        path = os.path.abspath(os.path.realpath(path))
        if self.cmd_args.fangdi:
            cs = (session.query(ht_models.CommunityFD)
                  .options(joinedload('district'))
                  .filter_by(track_presale=True)
                  .order_by(ht_models.CommunityFD.created_at)
                  .all())
            self.dump_fangdi(cs, os.path.join(path, 'fangdi.cvs'))
            
        
    def dump_fangdi(self, communities, path):
        with open(path, 'wb') as f:
            content = []
            content.append(u'编号,区县,名称,位置,公司,收录时间,链接\r\n')
            for c in communities:
                content.append('%s,%s,%s,%s,%s,%s,%s\r\n' % (
                                c.id,c.district.name, c.name, c.location,
                                c.company,c.created_at.strftime('%Y%m%d'),
                                c.community_url()))
            for l in content:
                f.write(l.encode('utf8'))
        print path
        

def confirm_result():
    session = get_session()
    # check price change
    sql = """select sum(case when T1.price > T2.price then 1 else 0 end) as rise_number,
                    sum(case when T1.price < T2.price then 1 else 0 end) as reduce_number,
                    sum(case when T1.price = T2.price
                                  and (T1.view_last_week > 0 or T1.view_last_month > 0)
                             then 1 else 0 end) as valid_unchange_number,
                    count(*) as house_available
             from house_record as T1 
             left join house_record as T2
               on T1.house_id = T2.house_id
               and T2.create_week = :create_week -1
             where T1.create_week = :create_week
         """
    h_record_join_aggr = session.execute(sql, {'create_week': week_number()}
                                         ).first()
    
    h_record_aggr = (session
                     .query(
                        func.count(HouseRecord.id).label('house_available'),
                        func.sum(case([(HouseRecord.price_change>0, 1)],
                                      else_=0)).label('rise_number'),
                        func.sum(case([(HouseRecord.price_change<0, 1)],
                                      else_=0)).label('reduce_number'),
                        func.sum(case([(and_(HouseRecord.price_change==0,
                                             or_(HouseRecord.view_last_week>0,
                                                 HouseRecord.view_last_month>0)),
                                         1)],
                                      else_=0)).label('valid_unchange_number'),
                        func.sum(HouseRecord.view_last_week).label('view_last_week')
                        )
                     .filter_by(create_week=week_number())
                     .one()
                    )
    
    for key in ('rise_number', 'reduce_number', 'valid_unchange_number'):
        try:
            assert int(getattr(h_record_join_aggr, key)) == int(getattr(h_record_aggr, key))
        except AssertionError:
            logger.error('%s in HouseRecord wrong.' % key) 
    logger.info('confirm rise_number, reduce_number, valid_unchange_number in '
                'HouseRecord finish.')
        
    # check house.available, house,new
    yesterday = (datetime.now() - timedelta(3)).strftime('%Y-%m-%d %H:%M:%S')
    house_aggr = (session
                  .query(func.sum(case([(House.created_at > yesterday, 1)],
                                       else_=0)
                                  ).label('create_number'),
                         func.sum(case([(House.available, 1)],
                                       else_=0)
                                  ).label('house_available'),
                         func.sum(case([(House.new, 1)],
                                       else_=0)).label('new_number'),
                         func.sum(case([(House.last_track_week==week_number()-1,
                                         1)],
                                       else_=0)).label('miss_number')
                         )
                  .one()
                  )
    
    try:
        assert int(house_aggr.create_number) == int(house_aggr.new_number)
    except AssertionError:
        logger.error('new_number in House wrong.')
    try:
        assert int(h_record_join_aggr.house_available) == int(house_aggr.house_available)
    except AssertionError:
        logger.error('house_available in House wrong.')
    logger.info('confirm new_number, house_available in House finishi.')
    
    # check community record
    c_record_aggr = (session
                     .query(func.sum(CommunityRecord.house_available).label('house_available'),
                            func.sum(CommunityRecord.rise_number).label('rise_number'),
                            func.sum(CommunityRecord.reduce_number).label('reduce_number'),
                            func.sum(CommunityRecord.valid_unchange_number).label('valid_unchange_number'),
                            func.sum(CommunityRecord.new_number).label('new_number'),
                            func.sum(CommunityRecord.miss_number).label('miss_number'),
                            func.sum(CommunityRecord.view_last_week).label('view_last_week'),
                            )
                     .filter_by(create_week=week_number())
                     .one()
                     )
    
    for key in ('house_available', 'rise_number', 'reduce_number', 
                'valid_unchange_number', 'view_last_week'):
        try:
            assert int(getattr(c_record_aggr, key)) == int(getattr(h_record_aggr, key))
        except AssertionError:
            logger.error('%s in CommunityRecord wrong' % key)
            
    for key in ('new_number', 'miss_number'):
        try:
            assert int(getattr(c_record_aggr, key) == getattr(house_aggr, key))
        except AssertionError:
            logger.error('%s in CommunityRecord wrong' % key)       
    logger.info('confirm CommunityRecord finish.')
        


    