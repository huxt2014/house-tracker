
import os
import sys
import importlib

from flask import Flask

version = '0.4.0'


root_path = os.path.dirname(os.path.abspath(__file__))

app = None

def get_application():
    from .utils import admin
    
    global app
    if app is None:
        app = Flask(__name__)
        admin.setup(app)
    return app

def run():
    pkg = 'house_tracker.commands'
    try:
        module = importlib.import_module('.'+sys.argv[1], pkg)
    except ImportError:
        module = importlib.import_module('.alembic', pkg)
        
    module.run()