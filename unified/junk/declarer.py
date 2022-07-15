from pythreader import TaskQueue, Task, synchronized, PyThread


class DeclareTask(Task, Logged):

    def __init__(self, config, file_desc, metadata, rse, url, sam_client, metacat_client, rucio_client):
        Task.__init__(self)
        Logged.__init__(self, name=f"DeclareTask({file_desc.Name})")
        self.Metadata = metadata
        self.Config = config
        self.FileDesc = file_desc
        self.URL = url
        self.SAMClient = sam_client
        self.MetaCatClient = metacat_client
        self.RucioClient = rucio_client

    def run(self):
        
        #
        # check if file in SAM
        #
        file_id = None
        if self.SAMClient is not None
            existing_sam_meta = self.SAMClient.get_file(self.FileDesc.Name)
            if existing_sam_meta is None:
                sam_metadata = self.Config.sam_metadata(self.FileDesc, metadata)
                sam_client.declare(sam_metadata)
                self.log("declared to SAM")
                existing_sam_meta = self.SAMClient.get_file(self.FileDesc.Name)
            else:
                self.log("exists in SAM")
            file_id = existing_sam_meta["file_id"]
            self.log("file id:", file_id)

        #
        # declare to MetaCat
        #
        if self.MetaCatClient is not None:
            did = self.Config.Scope + ":" + self.FileDesc.Name
            dataset = self.Config.Scope + ":" + self.Config.MetaCatDataset
            metacat_meta = self.Config.metacat_metadata(self.FileDesc, self.Metadata)   # massage meta if needed
            data = {
                        "name": self.Config.Scope + ":" + self.FileDesc.Name,
                        "metadata": metacat_meta,
                        "size":         self.Metadata.get("file_size"),
                        "checksums":    {   "adler32":  self.Metadata.get("checksum")   },
                    }
            if file_id is not None:
                data["fid"] = str(file_id)
            if not self.MetaCatClient.get_file(name=did):
                self.MetaCatClient.declare_files(dataset, [data])
                self.log("file declared")
            else:
                self.MetaCatClient.update_file_meta([data], mode="replace")
                self.MetaCatClient.add_files(dataset, [data])
                self.log("already exists in MetaCat - metadata replaced")
        
        #
        # declare to Rucio
        #
        if self.RucioClient is not None:
            
        # do the declarations
        #
    
        if sam_client is not None:
            sam_metadata = self.Config.sam_metadata(self.FileDesc, metadata)
            if sam_metadata:
                sam_client.declare(sam_metadata)
    

class Declarer(PyThread, Logged):

    MaxTasks = 10

    def __init__(self, config, sam_client, metacat_client, rucio_client):
        Logged.__init__(self)
        PyThread.__init__(self, name="Declarer", daemon=True)
        self.Config = config
        self.SAMClient = sam_client
        self.MetaCatClient = metacat_client
        self.RucioClient = rucio_client
        self.TaskQueue = TaskQueue(self.MaxTasks, stagger=0.5, delegate=self)

        
        
