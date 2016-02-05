import traceback
import sys
import os
import redis
from flask import abort, jsonify, request, Flask
from functools import wraps
from urlparse import urlparse

app = Flask(__name__)

pf = "coalesce.v1."

try:
    redis_url = urlparse(os.environ['REDIS_URL'])
except KeyError:
    traceback.print_exc()
    sys.exit(1)

rds = redis.Redis(host=redis_url.hostname,
                  port=redis_url.port,
                  password=redis_url.password,
                  decode_responses=True)


@app.errorhandler(400)
def handle_bad_request(error):
    response = jsonify({'message': error.description})
    response.status_code = 400
    return response


def ssl_required(fn):
    """
    Heroku terminates all SSL traffic before it reaches the application,
    but injects a header to indicate if the request originated from HTTPS.
    This decorator checks for the header HTTP_X_FORWARDED_PROTO and if none
    is found, returns a 400 Bad Request
    """
    @wraps(fn)
    def decorated_view(*args, **kwargs):
        if request.headers.get('HTTP_X_FORWARDED_PROTO'):
            return fn(*args, **kwargs)
        else:
            abort(400, 'HTTPS required; HTTP_X_FORWARDED_PROTO header missing')
        return fn(*args, **kwargs)
    return decorated_view


@app.route('/')
@ssl_required
def root():
    """
    GET: Return an index of available api
    """
    # TODO: return an index on available api
    return jsonify({'versions': ['v1']})


@app.route('/v1/list')
@ssl_required
def coalasce_lists():
    """
    GET: returns a list of all coalesced objects load into the listener
    """
    list_keys_set = rds.smembers(pf + "list_keys")
    if len(list_keys_set) == 0:
        return jsonify(**{pf: []})
    list_keys = [x for x in list_keys_set]
    return jsonify(**{pf: list_keys})


@app.route('/v1/stats')
@ssl_required
def stats():
    """
    GET: returns stats
    """
    pf_key = pf + 'stats'
    stats = rds.hgetall(pf_key)
    return jsonify(**stats)


@app.route('/v1/list/<key>')
@ssl_required
def list(key):
    """
    GET: returns list
    """
    pf_key = pf + 'lists.' + key
    coalesced_list = rds.lrange(pf_key, start=0, end=-1)
    return jsonify(**{key: coalesced_list})


if __name__ == '__main__':
    # TODO: remove debug arg
    app.run(host='0.0.0.0', debug=False)
