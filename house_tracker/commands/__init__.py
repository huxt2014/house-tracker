
import logging.config

from house_tracker.utils.conf_tool import GlobalConfig

class Command():
    
    def __init__(self, debug=False):
        logger_config = GlobalConfig().logger_config
        logger_config['root']['level'] = 'DEBUG' if debug else 'INFO'
        logging.config.dictConfig(logger_config)
        
    def run(self):
        raise Exception('should be override by subclass')

