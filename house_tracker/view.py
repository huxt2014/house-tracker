
import os
import logging

from flask import render_template, request, redirect, url_for

from . import get_application


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
        logger.info(request.form['username'])
        request.form['password']
        return redirect(url_for('login'))


