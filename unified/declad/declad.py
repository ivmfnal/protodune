from pythreader import PyThread, synchronized, Primitive, Task, TaskQueue
import json, hashlib, traceback, time, os, pprint
import rucio_client, metacat_client, samweb_client
from samweb_client import SAMDeclarationError
from logs import Logged

class MoverTask(Task, Logged):
    
    RequiredMetadata = ["checksum", "file_size", "runs"]
    
    def __init__(self, config, filedesc):
        Task.__init__(self, filedesc)
        Logged.__init__(self, name=f"MoverTask[{filedesc.Name}]")
        self.FileDesc = filedesc
        self.Config = config
        self.MetaSuffix = config.get("meta_suffix", ".json")
        self.RucioConfig = config.get("rucio", {})
        self.SAMConfig = config.get("samweb", {})
        self.QuarantineLocation = config.get("quarantine_location")
        self.SourceServer = config["source_server"]
        self.DestServer = config.get("destination_server") or self.SourceServer
        self.TransferTimeout = config.get("transfer_timeout", 120)
        self.Error = None
        self.Failed = False
        self.Status = "created"
        self.EventLog = []              # [(event, t, info), ...]
        self.EventDict = {}
        self.RetryAfter = None          # do not resubmit until this time
        self.KeepUntil = None           # keep in memory until this time
        self.timestamp("created")

    def last_event(self, name=None):
        if self.EventLog:
            if name is None:
                return self.EventLog[-1]
            else:
                last_record = (None, None, None)
                for event, t, info in self.EventLog:
                    if event == name:
                        last_record = (event, t, info)
                return last_record
        else:
            return None, None, None
            
    CoreAttributes = {
        "event_count":  "core.event_count",
        "file_type":    "core.file_type", 
        "file_format":  "core.file_format",
        "data_tier":    "core.data_tier", 
        "data_stream":  "core.data_stream", 
        "events":       "core.events",
        "first_event":  "core.first_event_number",
        "last_event":   "core.last_event_number",
        "event_count":  "core.event_count"
    }
    
    def metacat_metadata(self, desc, metadata):
        
        metadata = metadata.copy()      # so that we do not modify the input dictionary in place
        
        #
        # discard native file attributes
        #
        metadata.pop("file_size", None)
        metadata.pop("checksum", None)
        metadata.pop("file_name", None)

        out = {}
        #
        # pop out and convert core attributes
        #
        runs_subruns = set()
        run_type = None
        runs = set()
        for run, subrun, rtype in metadata.pop("runs", []):
            run_type = rtype
            runs.add(run)
            runs_subruns.add(100000*run + subrun)
        out["core.runs_subruns"] = sorted(list(runs_subruns))
        out["core.runs"] = sorted(list(runs))
        out["core.run_type"] = run_type

        for name, value in metadata.items():
            if '.' not in name:
                if name not in self.CoreAttributes:
                    raise ValueError("Unknown core metadata parameter: %s = %s for file %s", (name, value, desc.Name))
                name = self.CoreAttributes[name]
            out[name] = value
        
        out.setdefault("core.event_count", len(out.get("core.events", [])))
        
        return out

    def sam_metadata(self, desc, metadata):
        out = metadata.copy()
        out["file_name"] = desc.Name
        out["user"] = self.SAMConfig.get("user", os.getlogin())
        ck = out.get("checksum")
        if ck:
            if ':' in ck:
                type, value = ck.split(':', 1)
            else:
                type, value = "adler32", ck
        out["checksum"] = [f"{type}:{value}"]
        out.pop("events", None)
        #print("sam_metadata:"), pprint.pprint(out)
        return out

    def file_scope(self, desc, metadata):
        return metadata["runs"][0][2]

    def dataset_scope(self, desc, metadata):
        return self.file_scope(desc, metadata)

    def metacat_dataset(self, desc, metadata):
        return self.Config["metacat_dataset"]

    @property
    def name(self):
        return self.FileDesc.Name
        
    def rucio_dataset_did(self, desc, metadata):
        meta = metadata.copy()
        meta["run_number"] = meta["runs"][0][0]
        meta["run_type"] = meta["runs"][0][2]
        return self.RucioConfig["dataset_did_template"] % meta

    def undid(self, did):
        return did.split(":", 1)

    def destination_rel_path(self, scope, desc, metadata):
        """
        From Rucio lib/rucio/rse/protocols/protocol.py
        
        Given a LFN, turn it into a sub-directory structure using a hash function.

        This takes the MD5 of the LFN and uses the first four characters as a subdirectory
        name.

        :param scope: Scope of the LFN.
        :param name: File name of the LFN.
        :param rse: RSE for PFN (ignored)
        :param rse_attrs: RSE attributes for PFN (ignored)
        :param protocol_attrs: RSE protocol attributes for PFN (ignored)
        :returns: Path for use in the PFN generation.
        """
        name = desc.Name
        hstr = hashlib.md5(('%s:%s' % (scope, name)).encode('utf-8')).hexdigest()
        if scope.startswith('user') or scope.startswith('group'):
            scope = scope.replace('.', '/')
        return '%s/%s/%s/%s' % (scope, hstr[0:2], hstr[2:4], name)
        
    def dest_file_size(self, server, path):
        scanner = XRootDScanner(server, self.Config["scanner"])
        return scanner.getFileSize(path)

    def run(self):
        #self.debug("started")
        self.timestamp("started")
        self.Failed = False
        self.Error = None
        self.TaskStarted = time.time()
        #self.debug("time:", time.time())
        
        name, path = self.FileDesc.Name, self.FileDesc.Path
        #self.debug("name, path:", name, path)
        assert path.startswith("/")


        #
        # Get metadata and parse
        #
        
        meta_suffix = self.Config.get("meta_suffix", ".json")
        meta_tmp = self.Config.get("temp_dir", "/tmp") + "/" + self.FileDesc.Name + meta_suffix
        meta_path = path + meta_suffix
        download_cmd = self.Config["download_command_template"] \
            .replace("$server", self.SourceServer) \
            .replace("$src_path", meta_path) \
            .replace("$dst_path", meta_tmp)
        self.debug("download command:", download_cmd)

        self.timestamp("downloading metadata")

        ret, output = runCommand(download_cmd, self.TransferTimeout, self.debug)
        self.debug("metadata download command:", download_cmd)
        if ret:
            return self.failed("Metadata download failed: %s" % (output,))
        
        try:
            metadata = json.load(open(meta_tmp, "r"))
        except Exception as e:
            return self.failed(f"Metadata parse error: {e}")
        finally:
            os.remove(meta_tmp)

        self.debug("metadata downloaded:", metadata)
        
        if any (x not in metadata for x in self.RequiredMetadata):
            return self.failed("Not all required fields are present in metadata")

        try:    file_scope = self.file_scope(self.FileDesc, metadata)
        except Exception as e:
            return self.quarantine("can not get file scope. Error: %s. Metadata runs: %s" % (metadata.get("runs"),))
            
        did = file_scope + ":" + name
        file_size = metadata["file_size"]
        adler32_checksum = metadata["checksum"]
        if ':' in adler32_checksum:
            type, value = adler32_checksum.split(':', 1)
            assert type == "adler32"
            adler32_checksum = value
        dataset_scope = self.dataset_scope(self.FileDesc, metadata)
        
        #
        # Check file size
        #
        if file_size != self.FileDesc.Size:
            return self.quarantine(f"scanned file size {self.FileDesc.Size} differs from metadata file_size {file_size}")

        # EOS expects URL to have double slashes: root://host:port//path/to/file
        data_src_url = "root://" + self.SourceServer + "/" + path
        dest_root_path = self.Config["destination_root_path"]
        dest_rel_path = self.destination_rel_path(file_scope, self.FileDesc, metadata)
        dest_dir_abs_path = dest_root_path + "/" + dest_rel_path.rsplit("/", 1)[0]  
        dest_data_path = dest_root_path + "/" + dest_rel_path
        data_dst_url = "root://" + self.DestServer + "/" + dest_data_path     
        
        #
        # check if the dest data file exists and has correct size
        #
        
        try:    dest_size = self.dest_file_size(self.DestServer, dest_data_path)
        except Exception as e:
            return self.failed(f"Can not get file size at the destination: {e}")
            
        if dest_size is not None:
            self.debug(f"data file exists at the destination {dest_data_path}, size: {dest_size}")

        if dest_size != file_size:

            if dest_size is not None:
                self.log(f"destination file exists at {dest_data_path} but has incorrect size {dest_size} vs. {file_size}")

            #
            # copy data
            #
            create_dirs_command = self.Config["create_dirs_command_template"]   \
                .replace("$server", self.DestServer)    \
                .replace("$path", dest_dir_abs_path)
            self.debug("create dirs command:", create_dirs_command)

            self.timestamp("creating dirs")

            ret, output = runCommand(create_dirs_command, self.TransferTimeout, self.debug)
            if ret:
                return self.failed("Create dirs failed: %s" % (output,))

            copy_cmd = self.Config["copy_command_template"] \
                .replace("$dst_url", data_dst_url)    \
                .replace("$src_url", data_src_url)
            self.debug("copy command:", copy_cmd)

            self.timestamp("transferring data")

            self.debug("copy command:", copy_cmd)
            ret, output = runCommand(copy_cmd, self.TransferTimeout, self.debug)
            if ret:
                return self.failed("Data copy failed: %s" % (output,))

            self.log("data transfer complete")
        else:
            self.log("data file already exists at the destination and has correct size. Not overwriting")

        #
        # Do the declarations
        #
        #
        # declare to SAM
        #
        file_id = None
        
        sclient = samweb_client.client(self.SAMConfig)
        if sclient is not None:
            self.timestamp("declaring to SAM")
            existing_sam_meta = sclient.get_file(name)
            if existing_sam_meta is not None:
                sam_size = existing_sam_meta.get("file_size")
                sam_adler32 = dict(ck.split(':', 1) for ck in existing_sam_meta.get("checksum", [])).get("adler32").lower()
                if sam_size != file_size or adler32_checksum != sam_adler32:
                    return self.quarantine("already declared to SAM with different size and/or checksum")
                else:
                    self.log("already delcared to SAM with the same size/checksum")
            else:
                sam_metadata = self.sam_metadata(self.FileDesc, metadata)
                try:    file_id = sclient.declare(sam_metadata)
                except SAMDeclarationError as e:
                    return self.failed(str(e))
                self.log("declared to SAM with file id:", file_id)

        #
        # declare to MetaCat
        #
        mclient = metacat_client.client(self.Config)
        if mclient is not None:
            self.timestamp("declaring to MetaCat")
            existing_metacat = mclient.get_file(did=did)
            if existing_metacat:
                if existing_metacat["size"] != file_size or existing_metacat.get("checksums", {}).get("adler32") != adler32_checksum:
                    self.quarantine("already declared to MetaCat with different size and/or checksum")
                else:
                    self.log("already declared to MetaCat")
            else:
                dataset = self.metacat_dataset(self.FileDesc, metadata)
                metacat_meta = self.metacat_metadata(self.FileDesc, metadata)   # massage meta if needed
                file_info = {
                        "namespace":    file_scope,
                        "name":         name,
                        "metadata":     metacat_meta,
                        "size":         file_size,
                        "checksums":    {   "adler32":  adler32_checksum   },
                    }
                if file_id is not None:
                    file_info["fid"] = str(file_id)
                #print("about to call mclient.declare_files with file_info:", file_info)
                try:    mclient.declare_files(dataset, [file_info])
                except Exception as e:
                    return self.failed(f"MetaCat declaration failed: {e}")
                self.log("file declared to MetaCat")

        #
        # declare to Rucio
        #
        rclient = rucio_client.client(self.RucioConfig)
        if rclient is not None:
            from rucio.common.exception import DataIdentifierAlreadyExists, DuplicateRule, FileAlreadyExists
            #print(rclient.whoami())

            self.timestamp("declaring to Rucio")

            # create dataset if does not exist
            dataset_scope, dataset_name = self.undid(self.rucio_dataset_did(self.FileDesc, metadata))
            try:    rclient.add_did(dataset_scope, dataset_name, "DATASET")
            except DataIdentifierAlreadyExists:
                pass
            except Exception as e:
                return self.quarantine(f"Error in creating Rucio dataset {dataset_scope}:{dataset_name}: {e}")
                
            else:
                self.log(f"Rucio dataset {dataset_scope}:{dataset_name} created")

            for target_rse in self.RucioConfig["target_rses"]:
                try:
                    rclient.add_replication_rule([{"scope":dataset_scope, "name":dataset_name}], 1, target_rse)
                except DuplicateRule:
                    pass
                except Exception as e:
                    return self.quarantine(f"Error in creating Rucio replication rule -> {target_rse}: {e}")
                else:
                    self.log(f"replication rule -> {target_rse} created")
            
            # declare file replica to Rucio
            drop_rse = self.RucioConfig["drop_rse"]
            rclient.add_replica(drop_rse, file_scope, name, file_size, adler32_checksum)
            self.log(f"File replica declared in drop rse {drop_rse}")

            # add the file to the dataset
            try:
                rclient.attach_dids(dataset_scope, dataset_name, [{"scope":file_scope, "name":name}])
            except FileAlreadyExists:
                self.log("File was already attached to the Rucio dataset")
            else:
                self.log("File attached to the Rucio dataset")

        self.timestamp("removing sources")

        rmcommand = self.Config["delete_command_template"]	\
            .replace("$server", self.SourceServer)	\
            .replace("$path", meta_path)
        ret, output = runCommand(rmcommand, self.TransferTimeout, self.debug)
        if ret:
            return self.failed("Remove source metadata failed: %s" % (output,))

        rmcommand = self.Config["delete_command_template"]	\
            .replace("$server", self.SourceServer)	\
            .replace("$path", path)
        ret, output = runCommand(rmcommand, self.TransferTimeout, self.debug)
        if ret:
            return self.failed("Remove source ata failed: %s" % (output,))
        self.timestamp("complete")

    @synchronized
    def timestamp(self, event, info=None):
        self.EventDict[event] = self.LastUpdate = t =  time.time()
        self.EventLog.append((event, t, info))
        self.log(event)
        self.debug(event, "info:", info)
        self.Status = event

    @synchronized
    def failed(self, error):
        self.Failed = True
        self.Error = error
        self.log("failed with error:", error or "")
        self.timestamp("failed", error)

    @synchronized
    def quarantine(self, reason = None):
        # quarantine data only. we can leave the metadata in place
        self.Error = reason or self.Error
        self.Failed = True
        if self.QuarantineLocation:
            path = self.FileDesc.Path
            
            cmd = "xrdfs %s mv %s %s" % (
                self.FileDesc.Server,
                path,
                self.QuarantineLocation
            )
            self.debug("quarantine command for data %s: %s" % (self.FileDesc.Name, cmd))
            ret, output = runCommand(cmd, self.TransferTimeout, self.debug)
            if ret:
                return self.failed("Quarantine for data %s failed: %s" % (self.FileDesc.Name, output))
            self.timestamp("quarantined", self.Error)
        else:
            raise ValueError("Quarantine directory unspecified")
            
            
class Declad(FileProcessor):
    
    def __init__(self, config, history_db):
        FileProcessor.__init__(self, config, history_db)
        self.Config = config

    def create_task(self, filedesc):
        return MoverTask(self.Config, filedesc)
                    
    
