# coding=utf-8

import os
import argparse
import logging.config

from sqlalchemy.orm import joinedload
import alembic.config, alembic.util

from .workers import InitWorker, StartWorker
from .. import models
from ..config import Config
from ..utils.db import get_database_url, get_session


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
        
        self.batch_job_args = {'cache_dir':self.config.data_dir,
                               'interval_time': self.config.interval_time,
                               'clean_cache': self.cmd_args.clean_cache,
                               }
        self.targets = []
        if self.cmd_args.fangdi:
            self.targets.append(models.BatchJobFD)
        if self.cmd_args.lianjia:
            self.targets.append(models.BatchJobLJ)


class InitBatchJob(BatchJobCommand):
    @staticmethod
    def register_subcommand(subparsers):
        subparser = subparsers.add_parser('init')
        subparser.add_argument('--clean-cache', action='store_true')
        subparser.add_argument('--fangdi', action='store_true')
        subparser.add_argument('--lianjia', action='store_true')
        
    def start(self):
        workers = []
        for cls in self.targets:
            worker = InitWorker(cls, get_session(),
                                batch_job_args=self.batch_job_args)
            worker.start()
            workers.append(worker)
        
        for worker in workers:
            worker.join()


class StartBatchJob(BatchJobCommand):
    @staticmethod
    def register_subcommand(subparsers):
        subparser = subparsers.add_parser('start')
        subparser.add_argument('--clean-cache', action='store_true')
        subparser.add_argument('--fangdi', action='store_true')
        subparser.add_argument('--lianjia', action='store_true')
    
    def start(self):
        workers = []
        for cls in self.targets:
            worker = StartWorker(cls, get_session(),
                                 batch_job_args=self.batch_job_args)
            worker.start()
            workers.append(worker)
        
        for worker in workers:
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
            cs = (session.query(models.CommunityFD)
                  .options(joinedload('district'))
                  .filter_by(track_presale=True)
                  .order_by(models.CommunityFD.created_at)
                  .all())
            self.dump_fangdi(cs, os.path.join(path, 'fangdi.csv'))

    def dump_fangdi(self, communities, path):
        with open(path, 'wb') as f:
            content = []
            content.append(u'编号,区县,名称,位置,公司,收录时间,链接\r\n')
            for c in communities:
                content.append('%s,%s,%s,%s,%s,%s,%s\r\n' % (
                                c.id,c.district.name, c.name.replace(',',';'),
                                c.location, c.company,
                                c.created_at.strftime('%Y%m%d'),
                                c.community_url()))
            for l in content:
                f.write(l.encode('utf8'))
        print path
