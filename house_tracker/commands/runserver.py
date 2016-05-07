
import logging

from . import Command
from .. import view, get_application


import settings
logger = logging.getLogger(__name__)

class Runserver(Command):        
    def run(self):
        app = get_application()
        app.config['SECRET_KEY' ] = settings.SECRET_KEY
        app.config['SESSION_COOKIE_NAME' ] = settings.SESSION_COOKIE_NAME
        app.config['PERMANENT_SESSION_LIFETIME' ] = settings.PERMANENT_SESSION_LIFETIME
        app.run(host='0.0.0.0', port=8080)
    
def run():
    try:
        Runserver().run()
    except Exception as e:
        logger.exception(e)
        raise
    

