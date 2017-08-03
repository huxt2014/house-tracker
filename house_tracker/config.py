
import os
import copy
import tempfile

from sqlalchemy.engine.url import URL

from .utils import SingletonMeta
from .exceptions import ConfigError


DEFAULT_LOG_CONFIG = {
    'version': 1,
    'formatters': {
        'basic': {
            'format': '%(asctime)s-%(levelname)s:%(message)s'
            }
        },
    'handlers': {
        'file': {
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'basic',
            'filename': 'log.txt',
            'when': 'D',
            },
        'console': {
            'class': 'logging.StreamHandler',
            'formatter': 'basic',
            }
        },
    'root': {
        'level': 'INFO',
        'handlers': ['file']
        },
    'loggers': {
        # suppress the log info from requests to warn level
        'requests': {
            'level': 'WARN',
            'handlers': ['file'],
            'propagate': False
            },
        },
    'disable_existing_loggers': False
}


class Config(metaclass=SingletonMeta):

    log_file = None
    log_config = None
    data_dir = '/tmp/house_tracker'
    database = None
    db_url = None
    lj_number_per_page = 30
    email_list = None
    smtp = None

    def __init__(self):
        import house_tracker_settings
        for name in ('log_file', 'log_config', 'data_dir', 'database',
                     "lj_number_per_page", "smtp"):
            v = getattr(house_tracker_settings, name, None)
            if v is not None:
                setattr(self, name, v)

        email_list = getattr(house_tracker_settings, "email_list", None)
        if email_list is not None:
            if isinstance(email_list, list) or isinstance(email_list, tuple):
                self.email_list = [add for add in email_list]
            else:
                self.email_list = [email_list]

        self.check()

    def check(self):
        # log
        if self.log_config is None:
            self.log_config = copy.deepcopy(DEFAULT_LOG_CONFIG)
            if self.log_file is not None:
                self.log_config["handlers"]['file']['filename'] = \
                    real_path(self.log_file)

        # database
        if not self.database:
            raise ConfigError('no database config')
        else:
            try:
                self.db_url = URL(**self.database)
            except TypeError:
                raise ConfigError("invalid database config")

        # data_dir
        self.data_dir = real_path(self.data_dir, directory=True)


def real_path(path, directory=False):
    path = os.path.abspath(os.path.realpath(path))

    try:
        if not directory:
            d = os.path.basename(path)
            os.makedirs(d, exist_ok=True)
            f = tempfile.NamedTemporaryFile(dir=d)
            f.close()
        else:
            os.makedirs(path, exist_ok=True)
    except OSError as e:
        raise ConfigError("IO error: %s %s" % (path, e))

    return path
