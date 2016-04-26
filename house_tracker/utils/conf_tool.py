
import os
import sys

import house_tracker
from .decorators import singleton
from .exceptions import ConfigError

@singleton
class GlobalConfig():
    def __init__(self):
        self.root = '/'.join(os.path.abspath(house_tracker.__file__)
                             .split('/')[:-1])
        import settings
        for key, value in settings.__dict__.iteritems():
            if not key.startswith('_'):
                setattr(self, key, value)
        
        # check database configuration
        try:
            for key in ('driver', 'host', 'name', 'user', 'password'):
                self.database[key]
        except AttributeError:
            raise ConfigError('database server not configured yet.')
        except KeyError as e:
            raise ConfigError('database server configure error: %s missing', 
                              e.args[0])

        
        for key in ('log_dir', 'data_dir', 'time_interval', 'logger_config',
                    'original_date'):
            if not key in self.__dict__.keys():
                raise ConfigError('%s missing in setting file.' % key)