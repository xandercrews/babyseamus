import requests
from requests.auth import HTTPBasicAuth

import json

import cookielib

jar = cookielib.CookieJar()

class Object(object):
    pass

c = Object()
c.url = 'http://docker.demo.xcat:2480'
c.db = 'wat'
c.connect_url = '%s/connect/%s' % (c.url, c.db)
c.user = 'root'
c.passw = 'root'
c.command_url = '%s/command/%s/sql' % (c.url, c.db)
c.batch_url = '%s/batch/%s' % (c.url, c.db)
c.headers = {'content-type': 'application/json'}
c.dbclass = 'testclass'

def login():
    r = requests.get(c.connect_url, auth=HTTPBasicAuth(c.user, c.passw), cookies=jar)

    print r.status_code, r.text

    assert r.status_code == 204

def create_class(classname, extends=None):
    if extends:
        command('create class %s extends %s' % (classname, extends))
    else:
        command('create class %s' % classname)

def drop_class(classname):
    command('drop class %s' % classname)

def command(sql_command):
    r = requests.post(c.command_url, data=sql_command, headers=c.headers, auth=HTTPBasicAuth(c.user, c.passw), cookies=jar)

    print r.status_code, r.text

    assert 200 <= r.status_code < 300

def add_vertex(content, dbclass=c.dbclass):
    sql_command = "create vertex %s content %s" % (dbclass, json.dumps(content))

    print sql_command

    r = requests.post(c.command_url, data=sql_command, headers=c.headers, auth=HTTPBasicAuth(c.user, c.passw), cookies=jar)

    print r.status_code, r.text

    assert 200 <= r.status_code < 300

def batch_cmd(ops):
    r = requests.post(c.batch_url, data=ops, headers=c.headers, cookies=jar, auth=HTTPBasicAuth(c.user, c.passw))

    print r.status_code, r.text

    assert 200 <= r.status_code < 300
