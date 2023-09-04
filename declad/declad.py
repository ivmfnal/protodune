import pprint, time, threading, signal
from mover import Manager
from scanner import Scanner
from local_scanner import LocalScanner
from pythreader import PyThread, synchronized, Primitive, Task, TaskQueue
from config import Config
from logs import Logged, init as init_logger
import historydb
from web_server import App
from webpie import HTTPServer

GRACEFUL_SHUTDOWN_SIGNAL = signal.SIGHUP

class DeclaD(PyThread, Logged):
    
    def __init__(self, config, history_db):
        PyThread.__init__(self, name="DeclaD")
        Logged.__init__(self, "declad")
        self.Config = config
        self.HistoryDB = history_db
        self.MoverManager = Manager(config, self.HistoryDB)
        scanner_type = config["scanner"].get("type")
        if scanner_type == "local":
            self.Scanner = LocalScanner(self.MoverManager, config)
        elif scanner_type == "xrootd":
            self.Scanner = Scanner(self.MoverManager, config)
        else:
            raise ValueError(f"Unknown or unspecified scanner type: {scanner_type}")
        self.Stop = False
        
    def signal_handler(self, signum, frame):
        if signum == GRACEFUL_SHUTDOWN_SIGNAL:
            self.log("graceful shutdown signal received")
            self.stop()

    def stop(self):
        self.Stop = True
        self.wakeup()

    def run(self):
        self.HistoryDB.start()
        self.Scanner.start()
        self.MoverManager.start()
        while not self.Stop:
            self.sleep(100)
        self.log("stopping the manager ...")
        self.MoverManager.stop()
        self.log("waiting for the manager to finish ...")
        self.MoverManager.join()
        self.log("ending thread")

    def current_transfers(self):
        return self.MoverManager.current_transfers()

    def finished_transfers(self, limit=None):
        return list(self.HistoryDB.historySince(limit=limit))       # already reversed

    def quarantined(self):
        files, error = self.MoverManager.quarantined()		# file descriptors
        if files:
            status_records = self.HistoryDB.latest_records_bulk([d.Name for d in files])
            for d in files:
                d.LastRecord = status_records.get(d.Name)
        return files, error

    def recent_tasks(self):
        return self.MoverManager.recent_tasks()

    def task(self, name):
        return self.MoverManager.task(name)

    def ls_input(self):
        return self.Scanner.ls_input()

    def input_location(self):
        return self.Scanner.Server, self.Scanner.Location
        
class ThreadMonitor(PyThread, Logged):

    def __init__(self, interval):
        Logged.__init__(self, "ThreadMonitor")
        PyThread.__init__(self, name="ThreadMonitor", daemon=True)
        self.Interval = interval

    def run(self):
        while True:
            time.sleep(self.Interval)
            counts = {}
            for x in threading.enumerate():
                n = "%s.%s" % (x.__class__.__module__ or "", x.__class__.__name__)
                if isinstance(x, Primitive):
                        try:    n = x.kind
                        except: pass
                counts[n] = counts.get(n, 0)+1
            self.log("--- thread counts: ---")
            for n, c in sorted(counts.items()):
                self.log("    %-50s%d" % (n+":", c))

if __name__ == "__main__":
    import getopt, sys, os
    from logs import init_logger
    
    opts, args = getopt.getopt(sys.argv[1:], "dc:l:p:i")
    opts = dict(opts)
    
    if "-p" in opts:
        open(opts["-p"], "w").write("%d\n" % (os.getpid(),))
    
    config = Config(opts["-c"])
    debug = ("-d" in opts) or config.get("debug_enabled", False)

    log_out = opts.get("-l", config.get("log"))
    
    if "-i" in opts:
        # interactive - send all outout to stdout
        init_logger("-", error_out="-", debug_out="-", debug_enabled=debug)
    else:
        init_logger(log_out, error_out=config.get("error"), 
            debug_out=config.get("debug"),
            debug_enabled=debug
            )

    tm = ThreadMonitor(5*60)
    tm.start()

    history_db = historydb.open(config.get("history_db", "history.sqlite"))
    
    if "graphite" in config:
        from graphite_interface import GraphiteSender
        gsender = GraphiteSender(config["graphite"], history_db)
        gsender.start()

    declad = DeclaD(config, history_db)
    signal.signal(GRACEFUL_SHUTDOWN_SIGNAL, declad.signal_handler)
    web_config = config.get("web_gui", {})
    web_server = HTTPServer(web_config.get("port", 8080), App(web_config, declad, history_db),
            daemon=True)
    declad.start()
    web_server.start()
    declad.join()
    print("declad thread ended")
    

