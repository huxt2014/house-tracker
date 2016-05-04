
import os

from flask import Flask



root_path = os.path.dirname(os.path.abspath(__file__))

app = None

def get_application():
    global app
    if app is None:
        app = Flask(__name__)
    return app

 
