
from bottle import (route, post, get, static_file, request, TEMPLATE_PATH,
                    jinja2_view)

from ..utils.conf_tool import GlobalConfig

global_config = GlobalConfig()
TEMPLATE_PATH[:] = [global_config.root+'/static/template',]


@route('/login')
@jinja2_view('login.html')
def login():
    return {}

@post('/login')
def do_login():
    username = request.forms.get('username')
    password = request.forms.get('passowrd')
    if check_login(username, password):
        response.set_cookie('account', username, serect='cookie-key')
        return 

@get('/static/<filepath:path>')
def server_static(filepath):
    return static_file(filepath, root=global_config.root+'/static')


def check_login(username, password):
    pass