
import os

from flask import Flask

from .utils import admin


root_path = os.path.dirname(os.path.abspath(__file__))

app = None

def get_application():
    global app
    if app is None:
        app = Flask(__name__)
        admin.setup(app)
    return app

