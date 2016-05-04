
import logging

import bottle

from . import Command
from .. import view, get_application

logger = logging.getLogger(__name__)

class Runserver(Command):        
    def run(self):
        app = get_application()
        app.run(host='0.0.0.0', port=8080)
    
def run():
    try:
        Runserver().run()
    except Exception as e:
        logger.exception(e)
        raise
    

