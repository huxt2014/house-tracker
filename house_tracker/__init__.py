
import os

from flask import Flask
from flask_admin import Admin
from flask_admin.contrib.sqla import ModelView

from common.db import get_session
from .models import Community, House, CommunityRecord, HouseRecord


root_path = os.path.dirname(os.path.abspath(__file__))

app = None

def get_application():
    global app
    if app is None:
        app = Flask(__name__)
        admin = Admin(app, name='house_tracker', template_mode='bootstrap3')
        admin.add_view(ModelView(Community, get_session()))
        admin.add_view(ModelView(House, get_session()))
        admin.add_view(ModelView(CommunityRecord, get_session()))
        admin.add_view(ModelView(HouseRecord, get_session()))
    return app

 
