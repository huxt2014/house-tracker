import os
from datetime import date

database = {
    'driver': 'mysql',
    'host': 'localhost',
    'name': 'house_tracker',
    'user': 'terrence',
    'password': '123456',
    
    }

log_dir = '/var/log/house_tracker'

data_dir = '/var/data/house_tracker'

time_interval = 1

original_date = date(2016, 4, 13)

logger_config = {
    'version': 1,
    'formatters':{
        'basic': {
            'format': '%(asctime)s-%(name)s-%(levelname)s:%(message)s'
            }
        },
    'handlers':{
        'file':{
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'formatter': 'basic',
            'filename': os.path.join(log_dir, 'log.txt'),
            'when': 'D',
            },
        'console':{
            'class': 'logging.StreamHandler',
            'formatter': 'basic',
            'level': 'WARN'
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
            }       
        },
    # if True, the logger initialized before configuration happening will be disabled.
    'disable_existing_loggers':False
  
    }