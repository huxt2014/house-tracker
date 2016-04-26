
import logging

import bottle

from . import Command
from .. import views

logger = logging.getLogger(__name__)

class Runserver(Command):
    def __init__(self):
        Command.__init__(self)
        
    def run(self):
        bottle.run(host='0.0.0.0', port=8080)
    
def run():
    try:
        Runserver().run()
    except Exception as e:
        logger.exception(e)
        raise