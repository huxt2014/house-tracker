
import time
import logging

import requests

from ..exceptions import DownloadError

logger = logging.getLogger(__name__)


def singleton(cls):
    def inner_func(*args, **kwargs):
        if inner_func.instance is None:
            inner_func.instance = cls(*args, **kwargs)
        return inner_func.instance
        
    inner_func.instance = None
    return inner_func
    
    
class SingletonMeta(type):
    def __call__(cls, *args, **kwargs):
        if not hasattr(cls, 'instance'):
            cls.instance = type.__call__(cls, *args, **kwargs)
        return cls.instance


def do_http_request(session, request, timeout=5):

    try_times = 0

    while True:
        try_times += 1
        prepped = session.prepare_request(request)
        try:
            resp = session.send(prepped, timeout=timeout)
        except (requests.exceptions.RequestException,
                requests.exceptions.Timeout) as e:
            if try_times < 3:
                logger.warning("%s-th try failed for %s: %s",
                               try_times, request.url, e)
                if not isinstance(e, requests.exceptions.Timeout):
                    time.sleep(3)
            else:
                raise DownloadError("http request failed: %s, %s"
                                    % (prepped.url, e))
        else:
            break

    if resp.status_code != 200:
        raise DownloadError("bad http response: %s, %s",
                            resp.status_code, resp.url)

    return resp
