from pythreader import PyThread, synchronized, Primitive, Task, TaskQueue, LogFile
from configparser import ConfigParser
import json, sys, os, time, glob, sqlite3, traceback
from GraphiteInterface import GraphiteInterface
from Scanner import ScanManager
from tools import runCommand

Debug = False

def debug(msg):
    if Debug:
        print(msg)
        
class _HistoryDB(Primitive):

    def __init__(self, filename):
        Primitive.__init__(self)
        self.FileName = filename
        self.createTables()
        
    class DBConnectionGuard(object):

        def __init__(self, filename):
            self.DBConnection = sqlite3.connect(filename)

        def __enter__(self):
            return self.DBConnection

        def __exit__(self, exc_type, exc_value, traceback):
            self.DBConnection.close()
            self.DBConnection = None
            
    def dbconn(self):
        return _HistoryDB.DBConnectionGuard(self.FileName)

    def fetch_iter(self, c):
        tup = c.fetchone()
        while tup:
            yield tup
            tup = c.fetchone()

    @synchronized
    def createTables(self):
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("""
                create table if not exists config(
                    name text,
                    value, text,
                    primary key (name)
                )
                """)
            c.execute("""
                create table if not exists file_log(
                    filename text,
                    t float,
                    event text,
                    info text,
                    size bigint,
                    elapsed float,
                    primary key (filename, t))
                    """)
            c.execute("""
                create index if not exists file_log_fn_event_inx on file_log(filename, event)
                """)

    @synchronized
    def getConfig(self):
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("select name, value from config", ())
            return { name:value for name, value in c.fetchall() }
            
    @synchronized
    def setConfig(self, name, value):
        with self.dbconn() as conn:
            c = conn.cursor()
            config = self.getConfig()
            if name in config:
                c.execute("update config set value=? where name=?", (str(value), name))
            else:
                c.execute("insert into config(name, value) values(?, ?)", (name, str(value)))
            conn.commit()
            
    @synchronized
    def fileQueued(self, filename):
        #self.log("file queued: %s" % (filename,)
        self.addFileRecord(filename, "queued", "")     
           
    @synchronized
    def fileSucceeded(self, filename, size, elapsed):
        self.addFileRecord(filename, "done", "", size=size, elapsed=elapsed)
        
                    
    @synchronized
    def fileFailed(self, filename, reason):
        self.addFileRecord(filename, "failed", reason)
        
                    
    @synchronized
    def addFileRecord(self, filename, event, info, size=None, elapsed=None):
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("""
                insert into file_log(filename, t, event, info, size, elapsed) values(?,?,?,?,?,?)
                """, (filename, time.time(), event, info, size, elapsed))  
            conn.commit()    
            
    @synchronized
    def fileDone(self, fn):
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("select * from file_log where filename=? and event='done' limit 1", (fn,))
            done = not c.fetchone() is None
            #print "fileDone(%s) = %s" % (fn, done)
            return done
        
    @synchronized
    def removeFileDone(self, fn):
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("delete from file_log where filename=? and event='done'", (fn,))
            conn.commit()
            #c.execute("commit")
        
    @synchronized
    def historySince(self, t):
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("select filename, t, event, info from file_log where t >= ?", (t,))
            return c.fetchall()

    @synchronized
    def doneHistory(self, t):
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("""select filename, event, t, size, elapsed 
                            from file_log 
                            where t >= ?
                                and event = 'done'
                            order by t
            """, (t,))
            return c.fetchall()
        
            
    @synchronized
    def purgeOldRecords(self, before):
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("delete from file_log where t < ?", (before,))
            conn.commit()
            
    @synchronized
    def eventCounts(self, event_types, bin, since_t = None):
        event_types = ",".join(["'%s'" % (t,) for t in event_types])
        since_t = since_t or 0
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("""select event, round(t/?)*? as tt, count(*)
                    from file_log
                    where event in (%s) and t > ?
                    group by event, tt
                    order by event, tt""" % (event_types,), (bin, bin, since_t))
            return c.fetchall()

    @synchronized
    def getEvents(self, event_types, since_t = None):
        event_types = ",".join(["'%s'" % (t,) for t in event_types])
        since_t = since_t or 0
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("""select filename, event, t, size, elapsed
                    from file_log
                    where event in (%s) and t > ?
                    order by t""" % (event_types,), (since_t,))
            return c.fetchall()
                    #   group by filename

    @synchronized
    def historyForFile(self, filename):
        # returns [(t, event, info),...]
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("""
                select t, event, info 
                    from file_log 
                    where filename = ?
                    order by t
                    """, (filename,))
            return c.fetchall()


    @synchronized
    def historyByFile(self, filename=None, window=3*24*3600):
        # returns {filename: [(t, event, info),...]}
        with self.dbconn() as conn:
            t0 = time.time() - window
            c = conn.cursor()
            if filename is None:
                c.execute("""
                    select filename, t, event, info 
                        from file_log 
                        where t >= ?
                        order by filename, t
                        """, (t0,))
            else:
                c.execute("""
                    select filename, t, event, info 
                        from file_log 
                        where filename = ? and t >= ?
                        order by filename, t
                        """, (filename, t0))
            
            records = []
            filename = None
            lst = None
            for fn, t, event, info in self.fetch_iter(c):
                #print fn, t, event, info
                if fn == filename:
                    lst.append((t, event, info))
                else:
                    filename = fn
                    lst = [(t, event, info)]
                    records.append((filename, lst))
            out = sorted(records, key=lambda x: -x[1][-1][0])   # reversed by timestamp of the last element in the record list
            return out
            
HistoryDB = None
                
class FileMoverTask(Task):
    def __init__(self, manager, config, logger, filedesc):
        Task.__init__(self)
        self.ID = "%s.%s" % (id(self),time.time())
        self.Manager = manager
        filename = filedesc.Name
        self.FileDescriptor = filedesc
        self.FileName = filename
        self.Server = filedesc.Server
        self.Location  = filedesc.Location
        self.FilePath = filedesc.Path
        self.MetadataFileName = filename + ".json"
        self.MetadataFilePath = self.FilePath + ".json"
        self.Size = filedesc.Size
        self.TempDir = config.TempDir
        self.ChecksumRequired = config.ChecksumRequired
        self.UploadMetadata = config.UploadMetadata
        self.Ended = False
        self.Success = False
        self.Reason = None
        self.SourcePurge = config.SourcePurge
        self.Log = []       # [(timestamp, text),]
        self.Status = "starting"
        self.Created = time.time()
        self.Logger = logger
        self.TransferTimeout = config.TransferTimeout
        self.TransferTime = None
        self.TransferStarted = None
        #debug("%s created" % (self,))

    def __str__(self):
        return "[FileMoverTask(%s %s %s)]" % (
            self.Server, 
            self.Location, 
            self.FileName)
        
    def run(self):
        debug("Mover %s %s %s started" % (self.ID, self.FileName, self.Size))
        self.Status = "started"
        try:    self.do_run()
        except:
            self.failed("File mover exception: %s" % (traceback.format_exc(),))
        debug("Mover %s ended" % (self.FileName,))
        
    def log(self, msg):
        self.Logger.log("[%s]: %s" % (self.FileName, msg))
        self.Log.append(msg)
            
    def updateStatus(self, status):
        self.Status = status
        HistoryDB.addFileRecord(self.FileName, status, "")
        self.log(status)
        
    def do_run(self):
        self.log("started")
        self.TransferStarted = time.time()

        #
        # Get metadata and parse
        #
        
        self.updateStatus("downloading metadata")
        meta_tmp = self.TempDir + "/" + self.MetadataFileName
        download_cmd = self.Manager.metadataDownloadCommand(self.Server, self.MetadataFilePath, self.MetadataFileName, meta_tmp)
        ret, output = runCommand(download_cmd, self.TransferTimeout, debug)
        if ret:
            self.failed("Metadata download failed: %s" % (output,))
            return

        json_text = open(meta_tmp, "r").read()
        
        self.log("original metadata JSON: %s" % (json_text,))
        
        try:    metadata = json.loads(json_text)
        except: 
            self.log("metadata parsing error: %s" % (traceback.format_exc(),))
            self.log("metadata file contents -------\n%s\n----- end if metadata file contents -----" % (open(meta_tmp, "r").read(),))
            self.failed("Metadata parse error")
            return


        #
        # Copy data
        #
        self.updateStatus("transferring data")
        data_cp_command = self.Manager.dataCopyCommand(self.Server, self.FilePath, self.FileName)
        
        ret, output = runCommand(data_cp_command, self.TransferTimeout, debug)
        if ret:
            self.failed("Data transfer failed: %s" % (output,))
            return
        
        if self.UploadMetadata:
            # parse checksum here
            checksum = "unknown"
            if self.ChecksumRequired:
                for l in output.split("\n"):
                    l = l.strip()
                    if l.startswith("adler32:"):
                        value = l.split()[1]
                        if len(value) < 8:
                            value = ("0"*(8-len(value))) + value      # zero-pad to 8 hex digits
                        checksum = "adler32:" + value

            self.log("transfer checksum: %s" % (checksum,))

            if self.ChecksumRequired and checksum == "unknown":
                    self.failed("Error parsing checksum")
                    return

            #
            # add checksum to metadata
            #

            metadata["checksum"] = checksum    

            json_text = json.dumps(metadata)

            self.log("metadata JSON with checksum: %s" % (json_text,))

            open(meta_tmp, "w").write(json_text)

            #
            # Upload metadata
            #

            self.updateStatus("uploading metadata")
            upload_cmd = self.Manager.metadataUploadCommand(meta_tmp, self.MetadataFileName)
            #debug("upload command: %s" % (upload_cmd,))
            ret, output = runCommand(upload_cmd, self.TransferTimeout, debug)
            if ret:
                self.failed("Metadata upload failed: %s" % (output,))
                os.unlink(meta_tmp)
                return

            os.unlink(meta_tmp)

        if self.SourcePurge == "delete":

            #
            # Delete source
            #
            self.updateStatus("deleting source")

            cmd = self.Manager.deleteSourceCommand(self.Server, self.MetadataFilePath)
            ret, output = runCommand(cmd, self.TransferTimeout, debug)
            if ret:
                self.failed("Metadata file delete failed: %s" %(output,))
                return

            cmd = self.Manager.deleteSourceCommand(self.Server, self.FilePath)
            ret, output = runCommand(cmd, self.TransferTimeout, debug)
            if ret:
                self.failed("Data file delete failed: %s" %  (output,))
                return
                
        elif self.SourcePurge == "rename":
            #
            # Rename source
            #
            self.updateStatus("renaming source")

            cmd = self.Manager.renameSourceCommand(self.Server, self.MetadataFilePath)
            ret, output = runCommand(cmd, self.TransferTimeout, debug)
            if ret:
                self.failed("Metadata file rename failed: %s" %(output,))
                return

            cmd = self.Manager.renameSourceCommand(self.Server, self.FilePath)
            ret, output = runCommand(cmd, self.TransferTimeout, debug)
            if ret:
                self.failed("Data file delete failed: %s" %  (output,))
                return
            
        self.TransferTime = time.time() - self.TransferStarted                
        self.succeeded()
                
            
    def failed(self, reason):
        self.Reason = reason
        self.Ended = True
        self.Success = False
        self.log("failed: %s" % (reason,))
        self.Manager.moverFailed(self, reason)
        
    def succeeded(self):
        #debug("succeeded(%s)" % (self.FileName,))
        self.Ended = True
        self.Success = True
        self.log("done")
        self.Manager.moverSucceeded(self)
        
class MyConfigParser(ConfigParser):

    def get(self, section, param, default=None, **args):
        try:    return ConfigParser.get(self, section, param, **args)
        except: return default
        
class Configuration(object):
    def __init__(self, filename = None, envname = None):
        if not filename:
            filename = os.environ[envname]
        config = MyConfigParser()
        config.read(filename)
        self.Config = config
        self.CopyTemplate = config.get("Mover", "CopyTemplate")
        self.DownloadTemplate = config.get("Mover", "DownloadTemplate")
        self.UploadTemplate = config.get("Mover", "UploadTemplate")
        self.DeleteTemplate = config.get("Mover", "DeleteTemplate")
        self.RenameTemplate = config.get("Mover", "RenameTemplate")
        self.TempDir = config.get("Mover", "TempDir")
        self.RetryInterval = int(config.get("Mover", "RetryInterval", 30))
        self.KeepHistoryInterval = int(config.get("Mover", "KeepHistoryInterval", 3600*24))
        self.KeepLogInterval = int(config.get("Mover", "KeepLogInterval", 3600))
        self.MaxMovers = int(config.get("Mover", "MaxMovers", 10))
        self.SourcePurge = "none"
        self.DeleteSource = config.get("Mover", "DeleteSource", "no") == "yes"
        if self.DeleteSource:
            self.SourcePurge = "delete"         # default, "SourcePurge" will override
        self.SourcePurge = config.get("Mover", "SourcePurge", self.SourcePurge)
        self.LogFile = config.get("Mover", "LogFile")
        self.ChecksumRequired = config.get("Mover", "ChecksumRequired", "yes") == "yes"
        self.UploadMetadata = config.get("Mover", "UploadMetadata", "yes") == "yes"
        self.DatabaseFile = config.get("Mover", "DatabaseFile", "history.sqlite")


        self.TransferTimeout = int(config.get("Mover", "TransferTimeout", -1))
        self.StaggerInterval = float(config.get("Mover", "StaggerInterval", 0.0))
        
        self.NotifierHTTPPort = int(config.get("Scanner", "NotifierHTTPPort", 8090))

        self.ScanLocations = config.get("Scanner", "Locations", "").split()
        self.ScanServers = config.get("Scanner", "Servers", "local").split()

        self.ScanServersLocations = [
            (server, location) for server in self.ScanServers
                                for location in self.ScanLocations
        ]

        self.ScanRecursive = config.get("Scanner", "Recursive", "no") == "yes"

        prescale = float(config.get("Scanner", "PrescaleFactor", "1.0"))
        # round to nearest 1/100th
        prescale = float(int(prescale*100.0+0.5))/100.0
        self.ScanPrescale = prescale
        self.PrescaleSalt = config.get("Scanner", "PrescaleSalt", "")

        self.ScanInterval = int(config.get("Scanner", "ScanInterval", 20))
        self.lsCommandTemplate = config.get("Scanner", "lsCommandTemplate")

        fn_pattern = config.get("Scanner", "FilenamePattern", "")
        patterns = config.get("Scanner", "FilenamePatterns", fn_pattern)
        if not patterns:
            raise ValueError("filename patterns must be specified")
        self.FilenamePatterns = patterns.split()
        
        self.DirectoryRE = config.get("Scanner", "DirectoryRE", "^d")
        self.FileRE = config.get("Scanner", "FileRE", "^-")
        self.PathRE = config.get("Scanner", "PathRE", "[^ ]+$")
        self.SizeRE = config.get("Scanner", "SizeRE", "^[a-z-]+\s+[0-9-]+\s+\d\d:\d\d:\d\d\s*(?P<size>\d+)")
        self.ParseRE = config.get("Scanner", "ParseRE", "^(?P<type>[a-z-])\S+\s+\d+\s+\w+\s+\w+\s+(?P<size>\d+)\s+.+\s+(?P<path>\S+)$")
        self.ScannerOperationTimeout = float(config.get("Scanner", "OperationTimeout", 10.0))
        
        self.HTTPPort = int(config.get("Monitor", "HTTPPort", 8080))
        self.GUIPrefix = config.get("Monitor", "GUIPrefix", "/fts-light")
        if not self.GUIPrefix or self.GUIPrefix[0] != "/":
            self.self.GUIPrefix = "/" + self.GUIPrefix
        
        self.SendToGraphite = config.get("Graphite", "SendStats", "no") == "yes"
        if self.SendToGraphite:
            self.GraphiteHost = config.get("Graphite", "Host")
            self.GraphitePort = int(config.get("Graphite", "Port"))
            self.GraphiteNamespace = config.get("Graphite", "Namespace")
            self.GraphiteInterval = int(config.get("Graphite", "UpdateInterval"))
            self.GraphiteBin = int(config.get("Graphite", "Bin"))
            
        self.UserPasswords = {}
        for user in self.Config.options("Users"):
            self.UserPasswords[user] = self.Config.get("Users", user)
            
    def asList(self):
        sections = sorted(self.Config.sections())
        lst = []
        for s in sections:
            if s == "Users":    continue
            options = self.Config.options(s)
            slst = sorted([(k, self.Config.get(s, k)) for k in options])
            lst.append((s, slst))
        return lst
        
        
class Logger(Primitive):

    def __init__(self, logfile, time_to_keep):
        Primitive.__init__(self)
        self.Log = []       # (timestamp, message)
        self.LogFile = LogFile(logfile)
        self.TimeToKeep = time_to_keep

    @synchronized        
    def log(self, msg):
        t = time.time()
        self.Log.append((t, msg))
        self.LogFile.log("%s: %s" % (time.ctime(t), msg))
        
        #
        # purge log
        #
        
        i = 0
        while i < len(self.Log) and self.Log[i][0] < t - self.TimeToKeep:
            i += 1
        self.Log = self.Log[i:]
        
    def getLog(self):
        return self.Log[:]
        

class GraphiteSender(PyThread):

    Events = ("done", "discovered", "failed")

    def __init__(self, config):
        self.GraphiteHost = config.GraphiteHost
        self.GraphitePort = config.GraphitePort
        self.Namespace = config.GraphiteNamespace
        self.Interval = config.GraphiteInterval
        self.Bin = config.GraphiteBin
        self.GInterface = GraphiteInterface(self.GraphiteHost, self.GraphitePort, self.Namespace)
        PyThread.__init__(self)
        
    def run(self):
        while True:
            time.sleep(self.Interval)
            t0 = int(time.time() - self.Interval*10)/self.Bin*self.Bin
            counts = HistoryDB.eventCounts(self.Events, self.Bin, t0)
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

class Manager(PyThread):

    def __init__(self, config, held):
    
        PyThread.__init__(self)
        self.Config = config
        self.CopyTemplate = self.Config.CopyTemplate
        self.DownloadTemplate = self.Config.DownloadTemplate
        self.UploadTemplate = self.Config.UploadTemplate
        self.DeleteTemplate = self.Config.DeleteTemplate
        self.RenameTemplate = self.Config.RenameTemplate
        self.TempDir = self.Config.TempDir
        self.RetryInterval = self.Config.RetryInterval
        self.KeepHistoryInterval = self.Config.KeepHistoryInterval
        self.ChecksumRequired = self.Config.ChecksumRequired
        self.MaxMovers = self.Config.MaxMovers
        self.SourcePurge = self.Config.SourcePurge
        self.Logger = Logger(self.Config.LogFile, self.Config.KeepLogInterval)
        self.DatabaseFile = self.Config.DatabaseFile
        self.TransferTimeout = self.Config.TransferTimeout
        self.StaggerInterval = self.Config.StaggerInterval
        
        self.UserPasswords = config.UserPasswords
        
        
        self.RetryQueue = {}    # filename -> (retry time, desc)
        self.DoneHistory = {}   # filename -> (done time, desc)
        self.Log = []           # [(timestamp, filename, what)    

        self.Held = held

        self.MoverQueue = TaskQueue(self.MaxMovers, stagger = self.StaggerInterval)
        if self.Held:
            self.MoverQueue.hold()
                
        self.SendToGraphite = self.Config.SendToGraphite
        if self.SendToGraphite:
            self.GraphiteInterface = GraphiteInterface(self.Config.GraphiteHost, 
                self.Config.GraphitePort, self.Config.GraphiteNamespace)
                
        self.ScanMgr = ScanManager(self, config, held)

    def userPassword(self, username):
        return self.UserPasswords.get(username)          
        
    def getHistoryEventCounts(self, event_types, bin, since_t):
        return HistoryDB.eventCounts(event_types, bin, since_t)
        
    def getHistoryEvents(self, event_types, since_t):
        return HistoryDB.getEvents(event_types, since_t)
        
    def getConfig(self):
        return self.Config.asList()
    
    def log(self, what):
        self.Logger.log("Mover: %s" % (what,))
        
    def getLog(self):
        return self.Logger.getLog()
        
    def getHistory(self, filename=None):
        return HistoryDB.historyByFile(filename=filename, window=24*3600)
        
    @synchronized
    def hold(self):
        self.Held = True
        self.MoverQueue.hold()
        HistoryDB.setConfig("held", "yes")
        self.log("held")
     
    @synchronized
    def release(self):
        self.Held = False
        self.MoverQueue.release() 
        self.wakeup()
        HistoryDB.setConfig("held", "no")
        self.log("released")
     
    @synchronized
    def info(self):
        retry_queue = sorted(self.RetryQueue.values(), key = lambda item:   item[1].Name)     # sort by name
        done_history = HistoryDB.doneHistory(time.time() - 24*3600)     # (filename, event, tend, size, elapsed)      ordered by t
        queued, running = self.MoverQueue.tasks()
        queued = [m.FileDescriptor for m in queued]
        movers = sorted(running, key = lambda mover:   mover.FileName)
        #debug("queued: %s active: %s" % (queued, movers))
        return (movers, queued, retry_queue, done_history, [])
        
    @synchronized
    def knownFile(self, fn):
        return fn in self.DoneHistory or HistoryDB.fileDone(fn)
        
    def deleteSourceCommand(self, server, path):
        return self.DeleteTemplate\
            .replace("$server", server)\
            .replace("$path", path)
        
    def renameSourceCommand(self, server, path):
        return self.RenameTemplate\
            .replace("$server", server)\
            .replace("$path", path)
        
    def dataCopyCommand(self, server, path, filename):
        return self.CopyTemplate\
            .replace("$filename", filename)\
            .replace("$server", server)\
            .replace("$path", path)

    def metadataDownloadCommand(self, server, path, filename, dst):
        return self.DownloadTemplate\
            .replace("$filename", filename)\
            .replace("$dst", dst)\
            .replace("$server", server)\
            .replace("$path", path)
        
    def metadataUploadCommand(self, src, filename):
        return self.UploadTemplate.replace("$filename", filename).replace("$src", src)
        
    def retryLater(self, desc):
        filename = desc.Name
        t = self.RetryInterval + time.time()
        self.log("will retry %s after %s" % (filename, time.ctime(t)))
        self.RetryQueue[filename] = (t, desc)
        
        
    @synchronized
    def moverFailed(self, mover, reason):
        debug("Failed: %s %s" % (mover.FileName, reason))
        # update the DB
        HistoryDB.fileFailed(mover.FileName, reason)
        self.retryLater(mover.FileDescriptor)
        
    @synchronized
    def moverSucceeded(self, mover):
        debug("Succeeded: %s" % (mover.FileName, ))
        # update the DB
        self.DoneHistory[mover.FileName] = (time.time(), mover.FileDescriptor)
        HistoryDB.fileSucceeded(mover.FileName, mover.Size, mover.TransferTime)
        
    @synchronized                         
    def addFile(self, desc):
        mover_task = FileMoverTask(self, self.Config, self.Logger, desc)
        #, self.TempDir,
        #        self.SourcePurge, self.ChecksumRequired, self.TransferTimeout)
        self.MoverQueue.addTask(mover_task)
        self.log("file queued: %s" % (desc,))
        debug("Added to queue: %s" % (desc,))
        HistoryDB.fileQueued(desc.Name)
                
    @synchronized
    def newFile(self, filename):
        queued, running = self.MoverQueue.tasks()
        return not filename in [m.FileName for m in running] \
            and not filename in [m.FileName for m in queued] \
            and not filename in self.RetryQueue \
            and not self.knownFile(filename)
                
    @synchronized
    def updateRetryQueue(self):
        # just remove entries from the list so the scanner will re-discover them
        to_retry = []
        now = time.time()
        n = 0
        if not self.Held:
            for fn, (t, desc) in list(self.RetryQueue.items()):
                if t < now:
                    del self.RetryQueue[fn]
                    n += 1
        return n
                    
    @synchronized
    def retryNow(self, filename):
        self.log("retry now requested for %s" % (filename,))
        if filename in self.RetryQueue:
            t, desc = self.RetryQueue[filename]
            del self.RetryQueue[filename]
            self.wakeup()
            
    @synchronized
    def purgeHistory(self):
        now = time.time()
        for fn, (t, desc) in list(self.DoneHistory.items()):
            if t + 24*60*60 < now:
                del self.DoneHistory[fn]
        HistoryDB.purgeOldRecords(now - self.KeepHistoryInterval)

    def run(self):
        self.ScanMgr.start()
        while True:
            self.sleep(30)
            if not self.Held:
                if self.updateRetryQueue() > 0:
                    self.ScanMgr.needScan()
            self.purgeHistory()

        
if __name__ == "__main__":
    import getopt
    from GUI import GUIThread
    
    opts, args = getopt.getopt(sys.argv[1:], "c:d")
    opts = dict(opts)
    config = opts.get("-c") or os.environ.get("MOVER_CFG")
    if not config:
        print("Configuration file must be specified either with -c or using env. variable MOVER_CFG")
    config = Configuration(config)
    Debug = "-d" in opts
    
    HistoryDB = _HistoryDB(config.DatabaseFile)
    
    held = HistoryDB.getConfig().get("held", "no") == "yes"
    
    manager = Manager(config, held)
    
    #debug("Scanned locations: %s" % (config.ScanServersLocations,))
    
    home = os.path.dirname(__file__)
    gui = GUIThread(config, manager, manager.ScanMgr)
    gui.start()
  
    manager.start()
    
    if config.SendToGraphite:
        gsender = GraphiteSender(config)
        gsender.start()
    #web_service.start()

    manager.join()        
    
