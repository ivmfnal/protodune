from datetime import datetime
import jinja2, json, time
import os
from webpie import WPApp, Response, WPHandler, HTTPServer, WPStaticHandler
from Version import Version
from WebService import WSHandler

import time, math

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
        
        active, queue, retry, done = self.App.Manager.file_lists()
        states = [
            "queued",
            "starting",
            "transferring data",
            "downloading metadata",
            "uploading metadata",
            "to be retried",
            "done"
        ]
        
        files_in_states = {label:0 for label in states}   
        
        def add_file_in_state(state, n=1):
            files_in_states[state] = files_in_states.get(state, 0) + n

        for m in active:
            add_file_in_state(m.Status)
        add_file_in_state("queued", len(queue))
        add_file_in_state("to be retried", len(retry))
        add_file_in_state("done", len(done))

        return self.render_to_response("index.html", 
            states = states, files_in_states=files_in_states, 
            active=active, queue=queue, retry=retry, done=done)
        
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

    def transfer_rates(self, req, rel_path, since_t=None, **args):
        since_t = self.decode_time(since_t)
        data = self.App.HistoryDB.getEvents(["done"], since_t)
        points = [{
            "tend":     tend,
            "elapsed":  elapsed,
            "size":     size,
            } for _,_,tend,size,elapsed in data
        ]
        txt = json.dumps(points)
        def text_iter(text, chunk=1000000):
            for i in range(0, len(text), chunk):
                yield text[i:i+chunk]
        out = json.dumps({
            "tmin": since_t,
            "tmax": time.time(),
            "data": points
        })
        return text_iter(out), "text/json"

    def event_counts(self, req, rel_path, event_types=None, since_t=None, bin=None, **args):
        bin = self.decode_time(bin)   
        bin = max(int(bin), 1)
        #print "bin=",bin,"  since_t=",since_t
        tmin = int(self.decode_time(since_t)/bin)*bin
        tmax = int((time.time()+bin-1)/bin)*bin
        
        events = event_types.split(',')
        counts = {}
        event_counts = self.App.HistoryDB.eventCounts(events, bin, tmin)
        for event in events:
            counts[event] = dict((t,0) for t in range(tmin, tmax, bin))

        if event_counts:
            for event, t, n in event_counts:
                counts[event][t] = n

        out = {
            "events":   events,
            "tmin":     tmin,
            "tmax":     tmax,
            "rows": [
                [t] + [counts[e].get(t, 0) for e in events]
                for t in range(tmin, tmax+bin, bin)
            ]
        }

        return json.dumps(out), "text/json"

    
    def scanner_counts(self, req, rel_path, since_t=None, bin=None, **args):
        since_t = self.decode_time(since_t)
        bin = self.decode_time(bin)   
        bin = max(int(bin), 1)
        #print "bin=",bin,"  since_t=",since_t
        tmin = int(since_t/bin)*bin
        tmax = math.ceil(time.time()/bin)*bin

        nbins = (tmax-tmin)//bin
        zeros = [0]*nbins
        
        servers = set()
        locations = set()
        
        common_prefixes = {}            # {server -> common prefix so far}
        for record in self.App.HistoryDB.scannerHistorySince(since_t):
            i = int((record.T-tmin)/bin)
            if not record.Error:
                server = record.Server
                location = record.Location
                locations.add(location)
                servers.add(server)
                key = (server, location)
                if key not in counts:
                    counts[key] = zeros[:]
                    points[key] = zeros[:]
                counts[key][i] += record.NFiles
                points[key][i] += 1
                
                parts = location.split('/')
                common_prefix = common_prefixes.setdefault(server, parts)
                new_common = []
                for c, p in zip(common_prefix, parts):
                    if c == p:
                        new_common.append(c)
                common_prefixes[server] = new_common
                
        common_prefixes = {
            server: "" if not common_prefix else '/'.join(common_prefix)
            for server, common_prefix in common_prefixes.items()
        }

        averages = {}
        legends = {}
        for key in counts.keys():
            c = counts[key]
            n = points[key]
            a = [None]*nbins
            for i in range(nbins):
                if n[i] > 0:
                    a[i] = c[i]/n[i]
            server, location = key
            k = "%s:%s" % (server, location)
            averages[k] = a
            prefix = common_prefixes.get(server, "")
            legend = k
            if prefix and location.startswith(prefix):
                legend = "%s:...%s" % (server, location[len(prefix):])
            legends[k] = legend
            
        
        return json.dumps(
            {
                "tmin":     tmin,
                "tmax":     tmax,
                "bin":      bin,
                "legends":  legends,
                "keys":     sorted(list(averages.keys())),
                "locations":    sorted(list(locations)),
                "servers":      sorted(list(servers)),
                "averages":   averages
            }
        ), "text/json"
        
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

    def __init__(self, config, manager, scanmgr, history_db):
        self.URLPrefix = config.GUIPrefix
        WPApp.__init__(self, Handler, prefix=self.URLPrefix)
        self.Manager = manager
        self.ScanMgr = scanmgr
        self.SiteTitle = config.SiteTitle
        self.HistoryDB = history_db

    def init(self):
        print("App.init: self.URLPrefix:", self.URLPrefix)
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
                "GLOBAL_SiteTitle":self.SiteTitle,
                "GLOBAL_Version":Version,
                "GLOBAL_URLPrefix":self.URLPrefix
            }
        )

def GUIThread(config, manager, scanmgr, history_db):
    port = config.HTTPPort
    prefix = config.GUIPrefix
    app = App(config, manager, scanmgr, history_db)
    return HTTPServer(port, app)
                
            
                    
        
        
        
