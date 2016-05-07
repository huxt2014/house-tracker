
import os
import logging

from flask import render_template, request, redirect, url_for, session

from . import get_application


import settings
logger = logging.getLogger(__name__)
app = get_application()


@app.route('/')
def index():
    return redirect(url_for('login'))

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        return render_template('login.html')
    else:
        if (request.form['username'] == settings.USERNAME
            and request.form['password'] == settings.PASSWORD):
            session['pass'] = True
            if request.form.get('next'):
                return redirect(request.form['next'])
            else:
                return redirect(url_for('login'))
        else:
            if request.form.get('next'):
                render_template('login.html', status='failed', 
                                next=request.form['next'])
            else:
                return render_template('login.html', status='failed')


@app.before_request
def normal_before_request():
    session.permanent = True