from datetime import datetime
import jinja2, json, time
import os
from webpie import WPApp, Response, WPHandler, HTTPServer, WPStaticHandler
from Version import Version
from WebService import WSHandler

import time

class Handler(WPHandler):

    def __init__(self, request, app):
        WPHandler.__init__(self, request, app)
        self.WS = WSHandler(request, app)
        self.static = WPStaticHandler(request, app)

    def render_to_response(self, template, **args):
        params = {"held": self.App.Manager.Held}
        params.update(args)
        return WPHandler.render_to_response(self, template, **params)
        
    def index(self, req, rel_path, **args):
        movers, queue, retry, done, waiting = self.App.Manager.info()
        
        states = [
            "ready",
            "starting",
            "transferring data",
            "downloading metadata",
            "uploading metadata",
            "to be retried",
            "done"
        ]
        
        files_in_states = dict((label, []) for label in states)   
        
        def add_file_in_state(desc, state):
            #print state, files_in_states.get(state, "empty")
            if not state in files_in_states:
                files_in_states[state] = [desc]
            else:
                files_in_states[state].append(desc)
            files_in_states[state].sort()
        for m in movers:
            add_file_in_state(m.FileDescriptor, m.Status)
        for desc in queue:
            add_file_in_state(desc, "ready")
        for desc, rt in retry:
            add_file_in_state(desc, "to be retried")
        for filename, event, tend, size, elapsed in done:
            add_file_in_state(filename, "done")
        return self.render_to_response("index.html", 
            states = states, files_in_states=files_in_states, waiting=waiting,
            movers=movers, queue=queue, retry=retry, done=done)
        
    def log(self, req, rel_path, **args):
        log = reversed(self.App.Manager.getLog())
        return self.render_to_response("log.html", log=log)
        
    def config(self, req, rel_path, **args):
        config = self.App.Manager.getConfig()
        return self.render_to_response("config.html", config=config)
        
    def history(self, req, rel_path, filename=None, **args):
        history = self.App.Manager.getHistory(filename=filename)
        history = [(fn, len(lst), lst[0][0], lst) for fn, lst in history]
        return self.render_to_response("history.html", history=history)

            
    def decode_time(self, t):
        if t is None:   return t

        time_units = {
            's':    1,
            'm':    60,
            'h':    3600,
            'd':    3600*24
        }

        relative = t[0] == '-'
        if relative:    t = t[1:]

        if t[-1] in "smhd":
            t, unit = int(t[:-1]), t[-1]
            t = t*time_units[unit]
        else:
            t = int(t)
        if relative:    t = time.time() - t
        return t

    def transfer_rates(self, req, rel_path, since_t=None, bin=1, **args):
    
        bin = int(bin)
        since_t = self.decode_time(since_t)
        data = self.App.Manager.getHistoryEvents(["done"], since_t)
        points = [{
            "tend":     t,
            "elapsed":  elapsed,
            "size":     size,
            } for _,_,t,size,elapsed in data
        ]
        txt = json.dumps(points)
        def text_iter(text, chunk=1000000):
            for i in range(0, len(text), chunk):
                yield text[i:i+chunk]
        return Response(app_iter = text_iter(json.dumps(points)), content_type = "text/json")
        
    def event_counts(self, req, rel_path, event_types=None, since_t=None, bin=None, **args):
    
        bin = self.decode_time(bin)   
        bin = max(int(bin), 1.0)
        #print "bin=",bin,"  since_t=",since_t
        events = sorted(event_types.split(","))
        tmin = int(self.decode_time(since_t)/bin)*bin
        tmax = int((time.time()+bin-1)/bin)*bin
        event_counts = self.App.Manager.getHistoryEventCounts(events, bin, tmin)
        
        counts = {}
        for event in events:
            counts[event] = dict((t,0) for t in range(tmin, tmax, bin))
        
        if event_counts:
            for event, t, n in event_counts:
                tmin = t if tmin is None else min(t, tmin)
                tmax = t if tmax is None else max(t, tmax)
                counts[event][t] = n

        def table_to_json(counts, events, tmin, tmax, bin):
            yield '{ "events": [%s],\n' % (",".join(['"%s"' % (e,) for e in events]))
            yield '  "rows": [\n'
            for t in range(tmin, tmax+bin, bin):
                row = [t] + [counts[e].get(t, 0) for e in events]
                row = ",".join(["%s" % (x,) for x in row])
                comma = "," if t < tmax else ""
                yield "             [%s]%s\n" % (row, comma)
            yield "     ]\n}"
        return Response(app_iter = table_to_json(counts, events, tmin, tmax, bin),
            content_type = "text/json")
        
    def charts(self, req, rel_path, **args):
        return self.render_to_response("charts.html")
        
    def rate_histogram(self, req, rel_path, since_t=None, **args):
        since_t = self.decode_time(since_t)
        data = self.App.Manager.getHistoryEvents(["done"], since_t)
        rates = [size/elapsed for _,_,_,size,elapsed in data if elapsed > 0.0]
        range = 1000000.0           # 1 MB/s
        if rates:
            rmax = max(rates)
            while range < rmax:
                if range * 2 > rmax:
                    range *= 2
                    break
                if range * 5 > rmax:
                    range *= 5
                    break
                range *= 10
        nbins = 40
        bin = range / nbins
        hist = [0] * nbins
        #hist[0] = 1
        #hist[-1] = 1
        for r in rates:
            i = int((r+bin/2.0)/bin)
            hist[i] += 1
        out = {
            "range":    range,
            "data":     [{"rate":i*bin/1000000.0, "count":count} for i, count in enumerate(hist)]
        }
        return Response(json.dumps(out), content_type = "text/json")
        
    def input_dir(self, req, rel_path, **args):
        file_lists = []
        def relpath(p, t):
            if t and t[-1] != '/':
                t = t + "/"
            if p.startswith(t):
                return p[len(t):]
        for server, location, status, error, files in self.App.ScanMgr.listFiles(10):
            files = [relpath(f.Path, location) for f in files]            
            file_lists.append((server, location, status, error, sorted(files)))
        return self.render_to_response("input_dir.html", data=file_lists)
        
    def retry_now(self, req, rel_path, filename=None, **args):
        self.App.Manager.retryNow(filename)
        self.redirect('./index')
        
    def localtime(self, req, rel_path):
        localtime = time.localtime()
        return None #Response(json.dumps(

def pretty_time(t):
    if t == None:   return ""
    sign = ''
    if t < 0:   
        sign = '-'
        t = -t
    seconds = t
    if seconds < 60:
        out = '%.1fs' % (seconds,)
    elif seconds < 3600:
        seconds = int(seconds)
        minutes = seconds // 60
        seconds = seconds % 60
        out = '%sm%ss' % (minutes, seconds)
    else:
        seconds = int(seconds)
        minutes = seconds // 60
        hours = minutes // 60
        minutes = minutes % 60
        out = '%sh%sm' % (hours, minutes)
    return sign + out
    
def pretty_frequency(f):
    unit = ""
    if f > 0.0:
        unit = "/s"
        if f < 1.0:
            f = f*60.0
            unit = "/m"
            if f < 1.0:
                f = f*60.0
                unit = "/h"
        return "%.1f%s" % (f, unit)
    else:
        return ""

def host_port(addr):
    if not addr:    return ""
    return "%s:%d" % (addr[0], addr[1])
    
def time_delta(t1, t2=None):
    if not t1 or t1 == None:    return ""
    if t2 == None:  t2 = datetime.now()
    dt = t1 - t2
    seconds = dt.days * 3600 * 24 + dt.seconds + dt.microseconds/1000000.0
    if seconds < 0:  seconds = -seconds
    return pretty_time(seconds)
    
def dt_fmt(t):
    if t is None:   return ""
    if isinstance(t, (float, int)):
        t = datetime.fromtimestamp(t)
    return t.strftime("%m/%d/%y %H:%M:%S")
    
def none2null(x):
    return "null" if x == None else x
    
def pretty_size(x):
    if x > 10*1024:
        return "%.3f MB" % (float(x)/1024/1024,)
    elif x > 10:
        return "%.3f KB" % (float(x),)
    else:
        return "%d B" % (x,)
            
class App(WPApp):

    def __init__(self, url_prefix, manager, scanmgr):
        WPApp.__init__(self, Handler, prefix=url_prefix)
        self.Manager = manager
        self.ScanMgr = scanmgr
        self.URLPrefix = url_prefix

    def init(self):
        self.initJinjaEnvironment(
            tempdirs = [self.ScriptHome], 
            filters = {
                "pretty_size": pretty_size,
                "pretty_time": pretty_time,
                "pretty_frequency": pretty_frequency,
                "time_delta": time_delta,
                "dt_fmt": dt_fmt,
                "none2null": none2null
            }, 
            globals = {
                "GLOBAL_Version":Version,
                "GLOBAL_URLPrefix":self.URLPrefix
            }
        )

def GUIThread(config, manager, scanmgr):
    port = config.HTTPPort
    prefix = config.GUIPrefix
    app = App(prefix, manager, scanmgr)
    return HTTPServer(port, app)
                
            
                    
        
        
        
