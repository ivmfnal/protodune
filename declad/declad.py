from mover import Manager
from scanner import Scanner
from pythreader import PyThread, synchronized, Primitive, Task, TaskQueue
from config import Config
from logs import Logged
import historydb, pprint, time, threading
from web_server import App
from webpie import HTTPServer

class DeclaD(PyThread, Logged):
    
    def __init__(self, config, history_db):
        PyThread.__init__(self, name="DeclaD")
        Logged.__init__(self, "declad")
        self.Config = config
        self.HistoryDB = history_db
        self.MoverManager = Manager(config, self.HistoryDB)
        self.Scanner = Scanner(self.MoverManager, config)
        self.Stop = False

    def run(self):
        self.HistoryDB.start()
        self.Scanner.start()
        self.MoverManager.start()
        while not self.Stop:
            self.sleep(100)

    def current_transfers(self):
        return self.MoverManager.current_transfers()

    def finished_transfers(self):
        return list(self.HistoryDB.historySince())[::-1]

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
    import getopt, sys
    from logs import init_logger
    
    opts, args = getopt.getopt(sys.argv[1:], "dc:l:")
    opts = dict(opts)
    
    debug = "-d" in opts
    config = Config(opts["-c"])

    log_out = opts.get("-l", config.get("log"))
    
    init_logger(config.get("log"),
            debug, config.get("debug_out") or config.get("log") or ("-" if debug else None), 
            config.get("error") or config.get("log")
    )

    tm = ThreadMonitor(5*60)
    tm.start()

    history_db = historydb.open(config.get("history_db", "history.sqlite"))
    
    if "graphite" in config:
        from graphite_interface import GraphiteSender
        gsender = GraphiteSender(config["graphite"], history_db)
        gsender.start()

    declad = DeclaD(config, history_db)
    web_config = config.get("web_gui", {})
    web_server = HTTPServer(web_config.get("port", 8080), App(web_config, declad))
    declad.start()
    web_server.start()
    declad.join()
    

