

from . import get_application
from ..config import Config

config = Config()
config.debug = False

app = get_application(config)