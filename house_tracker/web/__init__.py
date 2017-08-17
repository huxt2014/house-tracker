
import functools

from flask import Flask, request, g
from werkzeug.contrib.cache import SimpleCache

from house_tracker import db

app = Flask(__name__)

cache = None


def get_application(config):
    from . import view
    db.init(config, debug=config.debug)

    global app, cache
    app.db_engine = db.engine

    if not config.debug:
        cache = SimpleCache()
    return app


# here is enough, no need for scoped_session
@app.before_request
def init_db_session():
    g.db_session = db.Session()


@app.teardown_request
def close_db_session(exc):
    g.db_session.close()


def cached(timeout=86400):

    def decorator(f):
        @functools.wraps(f)
        def inner(*args, **kwargs):
            if cache is None:
                return f(*args, **kwargs)
            else:
                key = request.url
                rv = cache.get(key)

                if rv is None:
                    rv = f(*args, **kwargs)
                    cache.set(key, rv, timeout=timeout)

                return rv

        return inner

    return decorator

