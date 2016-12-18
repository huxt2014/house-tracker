
import os
import logging.config

from .utils import singleton, SingletonMeta
from .exceptions import ConfigError



class Config():
    __metaclass__ = SingletonMeta
    
    def __init__(self):
        import house_tracker_settings
        for name in ('log_dir', 'data_dir', 'logger_config', 'database',
                     'interval_time'):
            setattr(self, name, getattr(house_tracker_settings, name, None))
    
    @property
    def log_dir(self):
        return self._log_dir
    @log_dir.setter
    def log_dir(self, value):
        if not value:
            self._log_dir = '/tmp'
        else:
            self._log_dir = value
            if not os.path.isdir(value):
                os.mkdir(value)
    
    @property
    def data_dir(self):
        return self._data_dir
    @data_dir.setter
    def data_dir(self, value):
        if not value:
            self._data_dir = '/tmp'
        else:
            self._data_dir = value
            if not os.path.isdir(value):
                os.mkdir(value)
    
    @property
    def logger_config(self):
        return self._logger_config
    @logger_config.setter
    def logger_config(self, value):
        if not value:
            self._logger_config = {
                'version': 1,
                'formatters':{
                    'basic': {
                        'format': ('%(asctime)s-%(name)s-%(lineno)d-'
                                   '%(levelname)s:%(message)s')
                        }
                    },
                'handlers':{
                    'file':{
                        'class': 'logging.handlers.TimedRotatingFileHandler',
                        'formatter': 'basic',
                        'filename': os.path.join(self.log_dir, 'log.txt'),
                        'when': 'D',
                        },
                    'console':{
                        'class': 'logging.StreamHandler',
                        'formatter': 'basic',
                        }
                    },
                'root':{
                    'level': 'INFO',
                    'handlers': ['file', 'console']
                    },
                'loggers':{
                    # suppress the log info from requests to warn level
                    'requests':{
                        'level': 'WARN',
                        'handlers': ['file'],
                        'propagate': False
                        },
                    },
                # if True, the logger initialized before configuration
                # happening will be disabled.
                'disable_existing_loggers':False
                }
        else:
            self._logger_config = value
        logging.config.dictConfig(self._logger_config)
    
    @property
    def database(self):
        return self._database
    @database.setter
    def database(self, value):
        try:
            for name in ('driver', 'host', 'name', 'user', 'password'):
                if not value.get(name):
                    raise ConfigError('database key missing: %s' % name)
        except AttributeError:
            raise ConfigError('database should be dict like.')
        else:
            self._database = value
            
    @property
    def interval_time(self):
        return getattr(self, '_interval_time', 1)
    @interval_time.setter
    def interval_time(self, value):
        try:
            self._interval_time = float(value)
        except ValueError:
            raise ConfigError('interval_time should be float')
    
    
    def __getattr__(self, name):
        raise ConfigError('%s not configured.' % name)
        
    
 