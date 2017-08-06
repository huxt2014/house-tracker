
from flask import Flask

from house_tracker import db

app = Flask(__name__)


def get_application(config, cmd_args):
    from . import view
    global app
    app.db_session = db.Session(config)
    app.db_engine = db.engine
    return app
