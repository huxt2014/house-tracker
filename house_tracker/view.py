
import os

from flask import render_template

from . import get_application



app = get_application()


@app.route('/login')
def login():
    return render_template('login.html')



