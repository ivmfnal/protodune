from webpie import WPApp, WPHandler
from version import Version
import pprint

class Handler(WPHandler):
    
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
        transfers = self.App.finished_transfers()
        return self.render_to_response("history.html", transfers=transfers)

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
        
    def finished_transfers(self):
        return self.Manager.finished_transfers()
        
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
    

