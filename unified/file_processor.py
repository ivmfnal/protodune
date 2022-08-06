from pythreader import PyThread, synchronized, Primitive, Task, TaskQueue
from logs import Logged

class FileProcessor(PyThread, Logged):
    
    def __init__(self, config, history_db):
        PyThread.__init__(self, daemon=True)
        Logged.__init__(self)
        self.Config = config
        capacity = config.get("queue_capacity", 100)
        max_movers = config.get("max_movers", 10)
        stagger = config.get("stagger", 0.5)
        self.TaskQueue = TaskQueue(max_movers, capacity=capacity, stagger=stagger, delegate=self)
        self.RetryCooldown = int(config.get("retry_interval", 3600))
        self.TaskKeepInterval = int(config.get("keep_interval", 24*3600))
        self.HistoryDB = history_db
        self.RecentTasks = {}	# name -> task
        self.Stop = False

    def task(self, name):
        return self.RecentTasks.get(name)

    def stop(self):
        self.Stop = True
        self.wakeup()
        
    def quarantined(self):
        qlocation = self.Config.get("quarantine_location")
        if not qlocation:
            return [], "Quarantine not configured"
        scanner = XRootDScanner(self.Config["source_server"], self.Config["scanner"])
        error = None
        files = []
        try:    files = scanner.scan(qlocation)		# returns file descriptors
        except Exception as e:
            error = str(e)
        return files or [], error
            
    @synchronized
    def recent_tasks(self):
        return sorted(self.RecentTasks.values(), key=lambda t: -t.last_event()[1] or 0)
        
    @synchronized
    def current_tasks(self):
        waiting, active = self.TaskQueue.tasks()
        return active + waiting

    def task_history(self):
        return list(self.HistoryDB.historySince())[::-1]
        
    # overridable
    def create_task(self, filedesc):
        return None

    @synchronized
    def create_task_if_new(self, name):
        waiting, active = self.TaskQueue.tasks()
        if any(t.name == name for t in waiting + active):
            return None         # active or queued
        task = self.RecentTasks.get(name)
        if task is None:
            task = self.RecentTasks[name] = self.create_task(filedesc)
        else:
            if task.RetryAfter is not None and task.RetryAfter > time.time():
                task = None
        return task

    def add_files(self, files):
        nfound = nqueued = 0
        for filedesc in files:
            nfound += 1
            name = filedesc.Name
            task = self.create_task_if_new(name)
            if task is not None:
                task.RetryAfter = time.time() + self.RetryCooldown
                task.KeepUntil = time.time() + self.TaskKeepInterval
                self.TaskQueue.addTask(task)            # this may block if the queue is at the capacity
                task.timestamp("queued")
                nqueued += 1
        self.log("%d new files queued out of %d found by the scanner" % (nqueued, nfound))

    def taskEnded(self, queue, task, _):
        if task.Failed:
            return self.taskFailed(queue, task, None, None, None)
        else:
            task.KeepUntil = time.time() + self.TaskKeepInterval
            task.RetryAfter = time.time() + self.RetryCooldown
            desc = task.FileDesc
            self.HistoryDB.file_done(desc.Name, desc.Size, task.Started, task.Ended)

    def taskFailed(self, queue, task, exc_type, exc_value, tb):
        task.KeepUntil = time.time() + self.TaskKeepInterval
        task.RetryAfter = time.time() + self.RetryCooldown
        desc = task.FileDesc
        #self.debug("task failed:", task, "   will retry after", time.ctime(self.RetryAfter[task.name]))
        if exc_type is not None:
            error = traceback.format_exception_only(exc_type, exc_value)
            self.log(f"Mover {desc.Name} exception:", error)
        else:
            # the error already logged by the task itself
            error = task.Error
        #self.debug("taskFailed: error:", error)
        if task.Status == "quarantined":
            self.HistoryDB.file_quarantined(desc.Name, task.Started, error, task.Ended)
        else:
            self.HistoryDB.file_failed(desc.Name, desc.Size, task.Started, error, task.Ended)

    def run(self):
        while not self.Stop:
            with self:
                self.RecentTasks = {name: task for name, task in self.RecentTasks.items() if task.KeepUntil >= time.time()}
            self.sleep(60)

class GraphiteSender(PyThread):

    Events = ("done", "discovered", "failed")

    def __init__(self, config, history_db):
        self.GraphiteHost = config.GraphiteHost
        self.GraphitePort = config.GraphitePort
        self.Namespace = config.GraphiteNamespace
        self.Interval = config.GraphiteInterval
        self.Bin = config.GraphiteBin
        self.GInterface = GraphiteInterface(self.GraphiteHost, self.GraphitePort, self.Namespace)
        self.HistoryDB = history_db
        PyThread.__init__(self)
        
    def run(self):
        while True:
            time.sleep(self.Interval)
            t0 = int(time.time() - self.Interval*10)/self.Bin*self.Bin
            counts = self.HistoryDB.eventCounts(self.Events, self.Bin, t0)
            out = {}        # { t: {event:count}, ... }
            #print "graphite: run ------------"
            totals = dict([(e,0) for e in self.Events])
            for event, t, count in counts:
                t = t + self.Bin/2
                if event in self.Events:
                    if not t in out:
                        out[t] = {}
                    #print "graphite: event: %s, t: %s, n: %d" % (event, t, count)
                    out[t][event] = float(count)/self.Bin
                    totals[event] += count
            #print "graphite: totals: %s" % (totals,)
            sys.stdout.flush()
            out = sorted(out.items())
            self.GInterface.send_timed_array(out)


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
