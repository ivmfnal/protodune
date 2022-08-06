from pythreader import PyThread, synchronized, Primitive, Task, TaskQueue
import json, sys, os, time, glob, traceback
from tools import runCommand
from fts3client import FTS3
from logs import Logged
from uid import uid

class FileMoverTask(Task, Logged):
    def __init__(self, manager, config, fts_client, filedesc):
        Task.__init__(self)
        self.ID = uid()
        self.LogName = f"FileMoverTask({self.ID})"
        Logged.__init__(self, self.LogName)
        self.FTSClient = fts_client
        self.Manager = manager
        filename = filedesc.Name
        self.FileDesc = filedesc
        self.MetaDesc = filedesc.metaDescriptor()
        self.TempDir = config.get("temp_dir", "/tmp")
        self.ChecksumRequired = config.get("checksum_required", True)
        self.Failed = False
        self.Error = None
        self.Status = "starting"
        self.Created = time.time()
        self.Log = MemoryLog()
        self.TransferTimeout = config.get("transfer_timeout", 300)
        self.log("created for:", filedesc.Path)
        self.Log.log(f"created id={self.ID}, file={self.FileDesc}, meta={self.MetaDesc}")

    def __str__(self):
        return "[FileMoverTask(%s, %s:%s)]" % (
            self.ID,
            self.FileDesc.Server, 
            self.FileDesc.Path
        )
        
    def run(self):
        self.debug("Mover %s %s %s started" % (self.ID, self.FileDesc.Name, self.FileDesc.Size))
        self.Status = "started"
        try:    
            self.do_run()
        except:
            self.failed("File mover exception: %s" % (traceback.format_exc(),))
        finally:
            self.Manager = None     # unlink circular dependency
        
    def log_record(self, *what):
        self.log(*what)
        self.Log.log(*what)

    def updateStatus(self, status):
        self.Status = status
        self.Manager.HistoryDB.addFileRecord(self.FileDesc.Name, status, "")
        self.log_record(status)
        
    def fts3transfer(self, filedesc, kind):
        src_url = self.sourceURL(filedesc)
        dst_url = self.sourceURL(filedesc)
        self.updateStatus(f"transferring {kind}")
        request = self.FTSClient.submit(src_url, dst_url)
        done = request.wait(self.TransferTimeout)
        if done:
            if request.Failed:
                return False, request.Error
        else:
            # timeout
            return False, "timeout"
        return True, None
        
    def remove_source(self, filedesc):
        remove_data_command = self.Manager.removeSourceDataCommand(filedesc)
        if remove_data_command:
            ret, output = runCommand(remove_data_command, self.TransferTimeout)
            if ret:
                return False, output
        return True, None
        
    def do_run(self):
        self.log_record("started")

        #
        # Get metadata and parse
        #
        self.updateStatus("validating metadata")
        meta_tmp = self.TempDir + "/" + self.MetaDescriptor.Name
        download_cmd = self.Manager.metadataDownloadCommand(self.MetaDescriptor, meta_tmp)
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
            self.log_record("metadata parsing error: %s" % (traceback.format_exc(),))
            self.log_record("metadata file contents -------\n%s\n----- end if metadata file contents -----" % (open(meta_tmp, "r").read(),))
            return self.failed("Metadata parse error")
        finally:
            try:
                os.remove(meta_tmp)
            except:
                pass

        self.debug("metadata validated")
        fts_client = FTS3(config.FTS3_URL, config.DelegatedProxy)

        #
        # transfer data and metadata
        #
        ok, error = self.fts3transfer(self.FileDesc, "data")
        if not ok:  return self.failed("Data transfer failed: " + error)

        ok, error = self.fts3transfer(self.MetaDesc, "metadata")
        if not ok:  return self.failed("Metadata transfer failed: " + error)

        #
        # remove input files
        #
        self.updateStatus("removing sources")

        ok, error = self.remove_source(self.FileDesc, "data")
        if not ok:  return self.failed("Data removing failed: " + error)
        
        ok, error = self.remove_source(self.MetaDesc, "metadata")
        if not ok:  return self.failed("Metadata removing failed: " + error)

        self.succeeded()

    def failed(self, error):
        self.Error = error
        self.Failed = True
        self.log_record("failed: %s" % (error,))

    def succeeded(self):
        #self.debug("succeeded(%s)" % (self.FileName,))
        self.log_record("done")
        
class IngestionD(FileProcessor):
        
    def __init__(self, config, history_db, fts_client):
        FileProcessor.__init__(self, config, history_db)
        self.Config = config
        self.DownloadTemplate = config["download_template"]
        self.RemoveSourceTemplate = config.get("remove_source_template")
        self.TempDir = config.get("temp_dir", "/tmp")
        self.RetryInterval = config.get("retry_interval", 60)
        self.FTSClient = fts_client
        self.SourceURLPattern = config["src_url_pattern"]
        self.DestinationURLPattern = config["dst_url_pattern"]
        self.RemoveSourceDataPattern = config.get("remove_source_pattern")
        self.RemoveSourceMetaPattern = config.get("remove_source_meta_pattern", self.RemoveSourceDataPattern)
        self.DownloadMetaTemplate = config["download_metadata_pattern"]

        # FTS3 client
        fts3_config = config["FTS3"]
        self.FTS3_URL = fts3_config["URL"]
        self.DelegatedProxy = fts3_config.get("delegated_proxy", os.environ.get("X509_USER_PROXY"))
        assert self.DelegatedProxy is not None, "X509 proxy delegated to FTS3 must be configured"

    def create_task(self, filedesc):
        return FileMoverTask(self, self.Config, FTS3(self.FTS3_URL, self.DelegatedProxy), filedesc)
        
    def expand_pattern(self, pattern, filedesc):
        return pattern                                  \
            .replace("$server", server)                 \
            .replace("$relpath", filedesc.Relpath)      \
            .replace("$location", filedesc.Location)    \
            .replace("$path", path)

    def sourceURL(self, filedesc):
        return self.expand_pattern(self.Config.SourceURLPattern, filedesc)

    def destinationURL(self, filedesc):
        return self.expand_pattern(self.Config.DestinationURLPattern, filedesc)

    def removeSourceCommand(self, filedesc):
        return self.expand_pattern(self.Config.RemoveSourcePattern, filedesc)

    def metadataDownloadCommand(self, filedesc, dst):
        return self.expand_pattern(self.Config.DownloadMetaTemplate, filedesc)  \
            .replace("$dst", dst)                       \

if __name__ == "__main__":
    import getopt, historydb, logs
    from GUI import GUIThread
    
    opts, args = getopt.getopt(sys.argv[1:], "c:dl:")
    opts = dict(opts)
    config = opts.get("-c") or os.environ.get("MOVER_CFG")
    if not config:
        print("Configuration file must be specified either with -c or using env. variable MOVER_CFG")
    config = Configuration(config)
    
    log_file = opts.get("-l", config.LogFile)
    logs.init_logger(log_file, debug_enabled = "-d" in opts)

    history_db = historydb.open(config.DatabaseFile)
    
    held = history_db.getConfig().get("held", "no") == "yes"

    manager = IngestionD(config, held, history_db)
    
    #debug("Scanned locations: %s" % (config.ScanServersLocations,))
    
    home = os.path.dirname(__file__)
    gui = GUIThread(config, manager, manager.ScanMgr, history_db)
    gui.start()
  
    manager.start()
    
    if config.SendToGraphite:
        gsender = GraphiteSender(config, history_db)
        gsender.start()
    #web_service.start()

    manager.join()        
