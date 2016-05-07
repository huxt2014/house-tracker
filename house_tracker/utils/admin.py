
from flask import session, request, redirect, url_for, render_template 
from flask_admin import Admin, AdminIndexView
from flask_admin.contrib.sqla import ModelView

from .db import get_session
from ..models import Community, House, CommunityRecord, HouseRecord


import settings

class SimpleAuthMixin(object):
    def is_accessible(self):
        return session.get('pass', False)
    
    def inaccessible_callback(self, name, **kwargs):
        return render_template('login.html', next=request.url)


class MyAdminIndexView(SimpleAuthMixin, AdminIndexView):
    pass
    

class BaseView(SimpleAuthMixin, ModelView):
    # remove fields from the create and edit forms
    form_excluded_columns = ['created_at', 'last_modified_at']
    

class CommunityView(BaseView):
    # remove fields from the create and edit forms
    form_columns = ['outer_id', 'name', 'district', 'area', 'last_track_week']
    column_searchable_list = ('outer_id', 'name')


class HouseView(BaseView):
    cloumn_default_sort = 'community_id'
    column_formatters = {
        'community': lambda v, c, m, p: m.community.name
    }

class CommunityRecordView(BaseView):
    cloumn_default_sort = 'community_id'
    column_formatters = {
        'community': lambda v, c, m, p: m.community.name
    }
    column_filters = ('community', 'create_week')


def setup(app):
    admin = Admin(app, name='house_tracker', template_mode='bootstrap3',
                  index_view=MyAdminIndexView())
    admin.add_view(CommunityView(Community, get_session()))
    admin.add_view(HouseView(House, get_session()))
    admin.add_view(CommunityRecordView(CommunityRecord, get_session()))
    admin.add_view(BaseView(HouseRecord, get_session()))