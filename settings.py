import os

database = {
    'driver': 'mysql',
    'host': 'localhost',
    'name': 'house_tracker',
    'user': 'terrence',
    'password': '123456',
    
    }

log_dir = '/var/log/house_tracker'

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
            'level': 'INFO'
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
    # if True, the logger initialized before configuration happening will be disabled.
    'disable_existing_loggers':False
  
    }