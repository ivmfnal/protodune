from webpie import WPApp, WPHandler, WPStaticHandler
from version import Version
import pprint, json, time

class Handler(WPHandler):
    
    def __init__(self, request, app):
        WPHandler.__init__(self, request, app)
        self.static = WPStaticHandler(request, app)

    def current(self, request, relpath, **args):
        transfers = self.App.current_transfers()
        return self.render_to_response("current.html", transfers=transfers)
        
    index = current

    def recent(self, request, relpath, **args):
        recent_tasks = sorted(self.App.recent_transfers(), key=lambda t: t.FileDesc.Name)
        #print("handler.recent: recent_tasks:", len(recent_tasks))
        return self.render_to_response("recent.html", recent_tasks=recent_tasks)

    def task_log(self, request, name, **args):
        task = self.App.task(name)
        return self.render_to_response("task_log.html", task=task)

    def history(self, request, relpath, **args):
        transfers = self.App.finished_transfers(limit=10000)
        return self.render_to_response("history.html", transfers=transfers)

    def charts(self, req, rel_path, **args):
        return self.render_to_response("charts.html")

    def quarantined(self, request, relpath, **args):
        files, error = self.App.quarantined()
        return self.render_to_response("quarantined.html", files=files, error=error)

    def ls_input(self, request, relpath, **args):
        files, error = self.App.ls_input()
        if files:
            files = sorted(files, key=lambda d: d.Name)
        server, location = self.App.input_location()
        return self.render_to_response("input_files.html", files=files, error=error,
            server=server, location=location)

    def config(self, request, relpath, **args):
        return self.render_to_response("config.html", 
            config=self.App.config(),
            formatted=pprint.pformat(self.App.config())
        )

    #
    # Data methods
    #
    
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
   
    def rate_histogram(self, req, rel_path, since_t=None, **args):
        since_t = self.decode_time(since_t)
        data = self.App.HistoryDB.getRecords("done", since_t)
        rates = [size/elapsed for _,_,_,size,elapsed in data if elapsed > 0.0]
        range = 1000000.0           # 1 MB/s
        if rates:
            rmax = max(rates)
            while range < rmax:
                range *= 2
                if range < rmax:
                    range *= 5
        nbins = 40
        bin = range / nbins
        hist = [0] * nbins
        #hist[0] = 1
        #hist[-1] = 1
        for r in rates:
            i = int(r/bin)
            hist[i] += 1
        out = {
            "xmin":     0,
            "xmax":     range,
            "bin":      bin,
            "data":     [{"rate":i*bin, "count":count} for i, count in enumerate(hist)]
        }
        return json.dumps(out), "text/json"
    
    def transfer_rates(self, req, rel_path, since_t=None, **args):
        since_t = self.decode_time(since_t)
        data = self.App.HistoryDB.getRecords("done", since_t)
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
        
    Events = ["done", "failed", "quarantined"]
        
    def event_counts(self, req, rel_path, event_types=None, since_t=None, bin=None, **args):
        bin = self.decode_time(bin)   
        bin = max(int(bin), 1)
        #print "bin=",bin,"  since_t=",since_t
        tmin = int(self.decode_time(since_t)/bin)*bin
        tmax = int((time.time()+bin-1)/bin)*bin
        event_counts = self.App.HistoryDB.eventCounts(bin, tmin)
        
        counts = {}
        events = set(self.Events) | set(event for event, _, _ in event_counts)
        events = sorted(list(events))
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
        


def as_dt_utc(t):
    from datetime import datetime
    if t is None:   return ""
    t = datetime.utcfromtimestamp(t)
    return t.strftime("%D&nbsp;%H:%M:%S")

def pretty_delta(t1, t2):
    if t1 is None or t2 is None:
        return ""
    t = abs(t2-t1)
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
    return out

def pretty_size(s):
    if s is None: return ""
    if s < 10*1024:
        return f"{s}B"
    elif s < 10*1024*1024:
        x = s/1024.0
        return "%.3fKB" % (x,)
    elif s < 10*1024*1024*1024:
        x = s/1024.0/1024.0
        return "%.3fMB" % (x,)
    else:
        x = s/1024.0/1024.0/1024.0
        return "%.3fGB" % (x,)
        
class App(WPApp):
    
    def __init__(self, config, manager, history_db):
        WPApp.__init__(self, Handler, prefix=config.get("prefix"))
        self.Manager = manager
        self.Config = config
        self.HistoryDB = history_db
        
    def init(self):
        templdir = self.ScriptHome + "/templates"
        self.initJinjaEnvironment(
            tempdirs=[templdir, "."],
            globals={
                "GLOBAL_Version": Version, 
                "GLOBAL_SiteTitle": self.Config.get("site_title", "Declaration Daemon"),
                "GLOBAL_URL_Prefix": self.Prefix or ""
            },
            filters = {
                "as_dt_utc": as_dt_utc,
                "pretty_delta": pretty_delta,
                "pretty_size": pretty_size
            }
        )
        
    def current_transfers(self):
        return self.Manager.current_transfers()
        
    def finished_transfers(self, limit=None):
        return self.Manager.finished_transfers(limit=limit)
        
    def quarantined(self):
        return self.Manager.quarantined()
        
    def ls_input(self):
        return self.Manager.ls_input()

    def input_location(self):
        return self.Manager.input_location()
        
    def recent_transfers(self):
        return self.Manager.recent_tasks()

    def task(self, name):
        return self.Manager.task(name)

    def config(self):
        return self.Manager.Config
    

