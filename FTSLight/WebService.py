from datetime import datetime
import os, re
from base64 import *
from webpie import Response, WPHandler
from tools import to_str, to_bytes

import time
from hashlib import md5

def md5sum(data):
    return md5(to_bytes(data)).hexdigest()

class WSHandler(WPHandler):

    def digestAuthorization(self, service, env, body):
        #print "authorizing..:"
        realm = "FTS-light.%s" % (service, )
        auth_header = env.get("HTTP_AUTHORIZATION","")
        matches = re.compile('Digest \s+ (.*)', re.I + re.X).match(auth_header)
        header = ""
        ok = False
        if matches:
            vals = re.compile(', \s*', re.I + re.X).split(matches.group(1))

            dict = {}

            pat = re.compile('(\S+?) \s* = \s* ("?) (.*) \\2', re.X)
            for val in vals:
                ms = pat.match(val)
                if ms:
                    dict[ms.group(1)] = ms.group(3)

            user = dict['username']
            cfg_password = self.App.Manager.userPassword(user)
            if not cfg_password:
                return False, None

            a1 = md5sum('%s:%s:%s' % (user, realm, cfg_password))
            a2 = md5sum('%s:%s' % (env['REQUEST_METHOD'], dict["uri"]))
            myresp = md5sum('%s:%s:%s:%s:%s:%s' % (a1, dict['nonce'], dict['nc'], dict['cnonce'], dict['qop'], a2))
            #print "response:   ", dict['response']
            #print "my response:", myresp
            ok = myresp == dict['response']
        else:
            #print "no matches"
            pass
        if not ok:
            nonce = b64encode(str(int(time.time())))
            header = 'Digest realm="%s", nonce="%s", algorithm=MD5, qop="auth"' % (realm, nonce)
        return ok, header

    def authenticate(self, req, service):
        body = req.body
        ok, header = self.digestAuthorization(service, req.environ, body)
        resp = None
        if not ok:
            resp = Response("Authorization required\n", status=401)
            if header:
                resp.headers['WWW-Authenticate'] = header
        return ok, resp

    def scan(self, req, rel_path, path=None, **args):     
        self.App.ScanManager.needScan()
        return Response("OK")
        
    def hold(self, req, rel_path, **args):
        ok, resp = self.authenticate(req, "control")
        if not ok:  return resp         # authentication failrure
        self.App.ScanMgr.hold()
        self.App.Manager.hold()
        return Response("OK")
        
    def release(self, req, rel_path, **args):
        #print "release"
        ok, resp = self.authenticate(req, "control")
        if not ok:  return resp         # authentication failrure
        self.App.Manager.release()
        self.App.ScanMgr.release()
        return Response("OK")

