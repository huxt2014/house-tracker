
import sys
import importlib
import logging.config

from house_tracker.utils.conf_tool import GlobalConfig 

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    logger_config = GlobalConfig().logger_config
    logging.config.dictConfig(logger_config)
    
    try:
        runner = importlib.import_module('.'+sys.argv[1],
                                         'house_tracker.commands')
        runner.run()
    except ImportError as e:
        logger.exception(e)
        print 'command not exist: %s' % sys.argv[1]