from flask.ext.restful import Api, Resource, reqparse
from flask import make_response
import flask
app = flask.Flask(__name__)
@app.after_request
def after_request(response):
    response.headers['Access-Control-Allow-Origin'] = '*'
    return response
api = Api(app)

@api.representation("text/html")
def out_html(data,code, headers=None):
    resp = make_response(data, code)
    resp.headers.extend(headers or {})
    return resp


if __name__ == '__main__':
    app.run(debug=True)
