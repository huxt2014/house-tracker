# coding=utf-8

import os
import sys
import copy
import argparse
import logging.config
from threading import Thread

import alembic.util
import alembic.config
from sqlalchemy.orm import joinedload
import gunicorn.app.wsgiapp

from . import models, mail, web
from .config import Config
from . import db


logger = logging.getLogger(__name__)


class Command:

    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='subcommand')

    # general
    parser.add_argument('-d', '--debug', action='store_true')

    # migrate
    alembic_cmd = alembic.config.CommandLine()
    subparsers._name_parser_map['migrate'] = alembic_cmd.parser

    # start
    subparser = subparsers.add_parser('start', help="start batch job")
    subparser.add_argument('--fangdi', action='store_true',
                           help="start batch job for fangdi.com")
    subparser.add_argument('--lianjia', action='store_true',
                           help="start batch job for lianjia.com")
    subparser.add_argument('-c', '--create', action='store_true',
                           help="create a new batch job")
    subparser.add_argument('-f', '--force', action='store_true',
                           help="create a new batch job even if the last batch"
                                " job not finished")

    # dump
    subparser = subparsers.add_parser('dump')
    subparser.add_argument('--fangdi', action='store_true')
    subparser.add_argument('-t', '--target', action='store', nargs='?')

    # runserver
    subparser = subparsers.add_parser('runserver', help="run web server")
    subparser.add_argument("-D", "--daemon", action="store_true",
                           help="run as daemon")

    def __new__(cls):
        cmd_args = cls.parser.parse_args()
        config = Config()
        for key, value in cmd_args.__dict__.items():
            setattr(config, key, value)

        logging.config.dictConfig(config.log_config)
        if cmd_args.debug:
            logging.getLogger().setLevel(logging.DEBUG)

        instance = None
        if cmd_args.subcommand == "migrate":
            instance = Migrate()
        elif cmd_args.subcommand == "start":
            instance = Start()
        elif cmd_args.subcommand == "dump":
            instance = Dump()
        elif cmd_args.subcommand == "runserver":
            instance = RunServer()

        instance.config = config
        instance.cmd_args = cmd_args

        return instance


class SubCommand:
    cmd_args = None
    config = None


class Migrate(SubCommand):

    def start(self):
        parser = Command.subparsers.choices['migrate']
        options = self.cmd_args
        if not hasattr(options, "cmd"):
            # see http://bugs.python.org/issue9253, argparse
            # behavior changed incompatibly in py3.3
            parser.error("too few arguments")
        else:
            cfg = alembic.config.Config(file_=options.config,
                                        ini_section=options.name,
                                        cmd_opts=options)
            cfg.set_main_option("script_location", "house_tracker:migrations")
            cfg.set_main_option("sqlalchemy.url", str(self.config.db_url))
            fn, positional, kwarg = options.cmd

            try:
                fn(cfg,
                   *[getattr(options, k, None) for k in positional],
                   **dict((k, getattr(options, k, None)) for k in kwarg)
                   )
            except alembic.util.CommandError as e:
                if options.raiseerr:
                    raise
                else:
                    alembic.util.err(str(e))


class Start(SubCommand):

    def start(self):
        db.init(self.config, debug=self.cmd_args.debug)
        self.cmd_args.auto_commit = True
        threads = []
        start_list = [(self.cmd_args.fangdi, models.BatchJobFD, []),
                      (self.cmd_args.lianjia, models.BatchJobLJ,
                       ["lj_number_per_page"])]

        mail.send_when_batch_job_done(self.config)

        for exist, cls, extra_params in start_list:
            if exist:
                kwargs = {"config": copy.deepcopy(self.config),
                          "cmd_args": copy.deepcopy(self.cmd_args)}
                for name in extra_params:
                    kwargs[name] = getattr(self.config, name)

                threads.append(Thread(target=models.BatchJob.run_batch,
                                      daemon=True, args=(cls,), kwargs=kwargs))

        for t in threads:
            t.start()

        for t in threads:
            t.join()


class RunServer(SubCommand):

    APP_MODULE = "house_tracker.web.wsgi:app"
    CONFIG = "python:house_tracker_settings"

    def start(self):
        if self.config.debug:
            app = web.get_application(self.config)
            app.run(host="127.0.0.1", port=8080, debug=True)
        else:
            self.run_gunicorn()

    def run_gunicorn(self):
        argv = ["gunicorn", "-c", self.CONFIG]
        if self.config.daemon:
            argv.append("--daemon")
        argv.append(self.APP_MODULE)
        sys.argv = argv
        gunicorn.app.wsgiapp.run()


class Dump(SubCommand):

    def start(self):
        db.init(self.config, debug=self.cmd_args.debug)
        session = db.Session()
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
        print(path)
