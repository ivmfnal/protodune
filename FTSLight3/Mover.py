from pythreader import PyThread, synchronized, Primitive, Task, TaskQueue, LogFile, LogStream
from configparser import ConfigParser
import json, sys, os, time, glob, traceback
from GraphiteInterface import GraphiteInterface
from Scanner import ScanManager
from tools import runCommand
from fts3client import FTS3
from logs import Logged
from uid import uid

class FileMoverTask(Task, Logged):
    def __init__(self, manager, config, fts_client, logger, filedesc):
        Task.__init__(self)
        self.ID = uid()
        self.LogName = f"MoverTask({filedesc.Name})"
        Logged.__init__(self, self.LogName)
        self.FTSClient = fts_client
        self.Manager = manager
        filename = filedesc.Name
        self.FileDescriptor = filedesc
        self.FileName = filename
        self.Server = filedesc.Server
        self.Location  = filedesc.Location

        self.FilePath = filedesc.Path
        self.FileRelpath = filedesc.Relpath         # path relative to the Location, with leading slash removed
        self.FileSrcURL = self.Manager.sourceURL(filedesc, self.FileRelpath)
        self.FileDstURL = self.Manager.destinationURL(filedesc, self.FileRelpath)

        self.MetadataFileName = filename + ".json"
        self.MetadataFilePath = self.FilePath + ".json"
        self.MetaRelpath = self.FileRelpath + ".json"
        self.MetaSrcURL = self.Manager.sourceURL(filedesc, self.MetaRelpath)
        self.MetaDstURL = self.Manager.destinationURL(filedesc, self.MetaRelpath)

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
        self.log("created for:", filedesc.Path)

    def __str__(self):
        return "[FileMoverTask(%s %s %s)]" % (
            self.Server, 
            self.Location, 
            self.FileName)
        
    def run(self):
        self.debug("Mover %s %s %s started" % (self.ID, self.FileName, self.Size))
        self.Status = "started"
        try:    self.do_run()
        except:
            self.failed("File mover exception: %s" % (traceback.format_exc(),))
        self.debug("Mover %s ended" % (self.FileName,))
        
    def log(self, *what):
        Logged.log(self, *what)
        self.Logger.log(self.LogName+":", *what)

    def updateStatus(self, status):
        self.Status = status
        self.Manager.HistoryDB.addFileRecord(self.FileName, status, "")
        self.log(status)
        
    def do_run(self):
        self.log("started")
        self.TransferStarted = time.time()

        #
        # Get metadata and parse
        #
        
        self.updateStatus("validating metadata")
        meta_tmp = self.TempDir + "/" + self.MetadataFileName
        download_cmd = self.Manager.metadataDownloadCommand(self.Server, self.MetadataFilePath, self.MetadataFileName, meta_tmp)
        ret, output = runCommand(download_cmd, self.TransferTimeout, self.debug)
        if ret:
            self.failed("Metadata download failed: %s" % (output,))
            return

        json_text = open(meta_tmp, "r").read()
        
        self.debug("original metadata JSON: %s" % (json_text,))
        
        try:    
            metadata = json.loads(json_text)
            if "file_size" not in metadata or "checksum" not in metadata:
                return self.failed("metadata does not include size or checksum")
        except: 
            self.log("metadata parsing error: %s" % (traceback.format_exc(),))
            self.log("metadata file contents -------\n%s\n----- end if metadata file contents -----" % (open(meta_tmp, "r").read(),))
            self.failed("Metadata parse error")
            return
        finally:
            try:
                os.remove(meta_tmp)
            except:
                pass

        self.debug("metadata validated")

        #
        # transfer data
        #
        self.updateStatus("transferring data")
        request = self.FTSClient.submit(self.FileSrcURL, self.FileDstURL)
        done = request.wait(self.TransferTimeout)
        if done:
            if request.Failed:
                msg = f"Data transfer failed: {request.Error}"
                self.log(msg)
                self.failed(msg)
                return
        else:
            # timeout
            msg = "Data transfer timeout"
            self.log(msg)
            self.failed(msg)
            return

        #
        # transfer metadata
        #
        self.updateStatus("transferring metadata")
        request = self.FTSClient.submit(self.MetaSrcURL, self.MetaDstURL)
        self.debug("Metadata transfer request submitted:", request.JobID)
        done = request.wait(self.TransferTimeout)
        if done:
            if request.Failed:
                msg = f"Metadata transfer failed: {request.Error}"
                self.log(msg)
                self.failed(msg)
                return
        else:
            # timeout
            msg = "Metadata transfer timeout"
            self.log(msg)
            self.failed(msg)
            return

        if self.SourcePurge == "delete":

            #
            # Delete source
            #
            self.updateStatus("deleting source")

            cmd = self.Manager.deleteSourceCommand(self.Server, self.MetadataFilePath)
            ret, output = runCommand(cmd, self.TransferTimeout, self.debug)
            if ret:
                self.failed("Metadata file delete failed: %s" %(output,))
                return

            cmd = self.Manager.deleteSourceCommand(self.Server, self.FilePath)
            ret, output = runCommand(cmd, self.TransferTimeout, self.debug)
            if ret:
                self.failed("Data file delete failed: %s" %  (output,))
                return

        elif self.SourcePurge == "rename":
            #
            # Rename source
            #
            self.debug("renaming the source...")
            self.updateStatus("renaming source")

            cmd = self.Manager.renameSourceCommand(self.Server, self.MetadataFilePath)
            ret, output = runCommand(cmd, self.TransferTimeout, self.debug)
            if ret:
                self.failed("Metadata file rename failed: %s" %(output,))
                return

            cmd = self.Manager.renameSourceCommand(self.Server, self.FilePath)
            ret, output = runCommand(cmd, self.TransferTimeout, self.debug)
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
        #self.debug("succeeded(%s)" % (self.FileName,))
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
        #self.CopyTemplate = config.get("Mover", "CopyTemplate")
        self.DownloadTemplate = config.get("Mover", "DownloadTemplate")
        #self.UploadTemplate = config.get("Mover", "UploadTemplate")
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

        self.SourceURLPattern = config.get("Mover", "SourceURLPattern")
        self.DestinationURLPattern = config.get("Mover", "DestinationURLPattern")
        self.RemovePathPrefix = config.get("Mover", "RemovePathPrefix", "/")

        self.FTS3_URL = config.get("FTS3", "URL")
        self.DelegatedProxy = config.get("FTS3", "delegated_proxy", os.environ.get("X509_USER_PROXY"))
        assert self.DelegatedProxy is not None, "X509 proxy delegated to FTS3 must be configured"

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
        #print("patterns:", self.FilenamePatterns)
        
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

    def __init__(self, time_to_keep):
        Primitive.__init__(self)
        self.Log = []       # (timestamp, message)
        self.TimeToKeep = time_to_keep

    @synchronized        
    def log(self, *what):
        t = time.time()
        msg = " ".join(str(x) for x in what)
        self.Log.append((t, msg))
        
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

class Manager(PyThread, Logged):

    def __init__(self, config, held, history_db):
        self.LogName = "Manager"
        Logged.__init__(self, self.LogName)
    
        PyThread.__init__(self)
        self.HistoryDB = history_db
        self.Config = config
        #self.CopyTemplate = self.Config.CopyTemplate
        self.DownloadTemplate = self.Config.DownloadTemplate
        #self.UploadTemplate = self.Config.UploadTemplate
        self.DeleteTemplate = self.Config.DeleteTemplate
        self.RenameTemplate = self.Config.RenameTemplate
        self.TempDir = self.Config.TempDir
        self.RetryInterval = self.Config.RetryInterval
        self.KeepHistoryInterval = self.Config.KeepHistoryInterval
        self.ChecksumRequired = self.Config.ChecksumRequired
        self.MaxMovers = self.Config.MaxMovers
        self.SourcePurge = self.Config.SourcePurge
        self.Logger = Logger(self.Config.KeepLogInterval)
        self.DatabaseFile = self.Config.DatabaseFile
        self.TransferTimeout = self.Config.TransferTimeout
        self.StaggerInterval = self.Config.StaggerInterval

        self.FTS3 = FTS3(config.FTS3_URL, config.DelegatedProxy)
        
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
        self.debug("Manager created. Held=", held)
        
    def userPassword(self, username):
        return self.UserPasswords.get(username)          
        
    def getHistoryEventCounts(self, event_types, bin, since_t):
        return self.HistoryDB.eventCounts(event_types, bin, since_t)
        
    def getHistoryEvents(self, event_types, since_t):
        return self.HistoryDB.getEvents(event_types, since_t)
        
    def getConfig(self):
        return self.Config.asList()
    
    def log(self, *what):
        Logged.log(self, *what)
        self.Logger.log(self.LogName + ":", *what)
        
    def getLog(self):
        return self.Logger.getLog()
        
    def getHistory(self, filename=None):
        return self.HistoryDB.historyByFile(filename=filename, window=24*3600)
        
    @synchronized
    def hold(self):
        self.Held = True
        self.MoverQueue.hold()
        self.HistoryDB.setConfig("held", "yes")
        self.log("held")
     
    @synchronized
    def release(self):
        self.Held = False
        self.MoverQueue.release() 
        self.wakeup()
        self.HistoryDB.setConfig("held", "no")
        self.log("released")
     
    @synchronized
    def file_lists(self):
        retry_queue = sorted(self.RetryQueue.values(), key=lambda x: x[1].Path)
        queued, running = self.MoverQueue.tasks()
        done = self.HistoryDB.doneHistory(time.time() - 24*3600)	# (filename, event, tend, size, elapsed)      ordered by t
        #self.debug("queued: %s active: %s" % (queued, movers))
        return (running, queued, retry_queue, done)
        
    @synchronized
    def knownFile(self, fn):
        return fn in self.DoneHistory or self.HistoryDB.fileDone(fn)
        
    def sourceURL(self, filedesc, relpath):
        return self.Config.SourceURLPattern             \
            .replace("$relpath", relpath)               \
            .replace("$location", filedesc.Location)    \
            .replace("$server", filedesc.Server)

    def destinationURL(self, filedesc, relpath):
        return self.Config.DestinationURLPattern        \
            .replace("$relpath", relpath)               \
            .replace("$location", filedesc.Location)    \
            .replace("$server", filedesc.Server)

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
        self.debug("Failed: %s %s" % (mover.FileName, reason))
        # update the DB
        self.HistoryDB.fileFailed(mover.FileName, reason)
        self.retryLater(mover.FileDescriptor)
        
    @synchronized
    def moverSucceeded(self, mover):
        self.debug("Succeeded: %s" % (mover.FileName, ))
        # update the DB
        self.DoneHistory[mover.FileName] = (time.time(), mover.FileDescriptor)
        self.HistoryDB.fileSucceeded(mover.FileName, mover.Size, mover.TransferTime)
        
    @synchronized                         
    def addFile(self, desc):
        mover_task = FileMoverTask(self, self.Config, self.FTS3, self.Logger, desc)
        #, self.TempDir,
        #        self.SourcePurge, self.ChecksumRequired, self.TransferTimeout)
        self.MoverQueue.addTask(mover_task)
        self.log("file queued: %s" % (desc,))
        self.debug("Added to queue: %s" % (desc,))
        self.HistoryDB.fileQueued(desc.Name)
                
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
        self.HistoryDB.purgeOldRecords(now - self.KeepHistoryInterval)

    def run(self):
        self.ScanMgr.start()
        while True:
            self.sleep(30)
            if not self.Held:
                if self.updateRetryQueue() > 0:
                    self.ScanMgr.needScan()
            self.purgeHistory()

        
if __name__ == "__main__":
    import getopt, historydb
    from GUI import GUIThread
    import logs
    
    opts, args = getopt.getopt(sys.argv[1:], "c:dl:")
    opts = dict(opts)
    config = opts.get("-c") or os.environ.get("MOVER_CFG")
    if not config:
        print("Configuration file must be specified either with -c or using env. variable MOVER_CFG")
    config = Configuration(config)
    
    log_file = opts.get("-l", config.LogFile)
    logs.init_logger(log_file, "-d" in opts)

    history_db = historydb.open(config.DatabaseFile)
    
    held = history_db.getConfig().get("held", "no") == "yes"

    
    manager = Manager(config, held, history_db)
    
    #debug("Scanned locations: %s" % (config.ScanServersLocations,))
    
    home = os.path.dirname(__file__)
    gui = GUIThread(config, manager, manager.ScanMgr)
    gui.start()
  
    manager.start()
    
    if config.SendToGraphite:
        gsender = GraphiteSender(config, history_db)
        gsender.start()
    #web_service.start()

    manager.join()        
