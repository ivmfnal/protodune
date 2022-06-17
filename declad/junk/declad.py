from pythreader import synchronized, PyThread
from logs import Logged
from rucio.client.replicaclient import ReplicaClient
import yaml

class ReplicaChecker(PyThread, Logged):
    
    DefaultInterval = 30       # seconds
    
    def __init__(self, mover, config, interval=None):
        PyThread.__init__(self, name="ReplicaChecker")
        Logged.__init__(self, name="ReplicaChecker")
        self.Interval = interval or self.DefaultInterval
        self.RucioClient = ReplicaClient()
        self.Stop = False
        self.Mover = mover
        self.TargetRSE = config["target_rse"]

    def run(self):
        while not self.Stop:
            dids = self.Mover.dids()      # [{"scope":"...", "name":"..."}, ...]
            replicas = self.RucioClient.list_replicas(dids, rse_expression=self.TargetRSE)
            done = []
            for r in replicas:
                if self.TargetRSE in r["rses"]:
                    done.append(r["scope"] + ":" + r["name"])
            self.log("transfers complete:", len(done), "   still pending:", len(dids)-len(done))
            if done:
                self.Mover.transfers_complete(done)
            if not self.Stop:
                time.sleep(self.Interval)
                
class Declarer(PyThread, Logged):

    DefaultInterval = 30       # seconds

    def __init__(self, config, mover, interval):
        Primitive._init__(self, name="Manager")
        self.Scope = config["scope"]
        self.DatasetPattern = config["dataset_pattern"]
        self.MetacatClient = MetaCatClient(config["metacat_url"])
        self.SAMWebClient = SAMWebClient(config["samweb_url"])
        self.ToDeclare = {}          # {did -> metadata}
        self.DatasetScope = config.get("dataset_scope", self.Scope)
        self.MetaCatDataset = config["metacat_dataset"]
        
    @synchronized
    def add_file(self, did, path, metadata):
        if did not in self.PendingFiles:
            self.ToDeclare[did] = (path, metadata)

    @synchronized
    def do_declare(self):
        #
        # Declare files to MetaCat
        #
        datasets_files = {}     # {dataset name -> [file items]}
        for did, (path, metadata) in self.ToDeclare.items():
            namespace, name = did.split(":", 1)
            meta = metadata.copy()
            size = meta.pop("size", None)
            checksum = meta.pop("checksum", None)
            dataset = self.DatasetPattern % meta
            
            datasets_files.setdefault(dataset, []).append(dict(
                size=size,
                checksums={"adler32":checksum},
                metadata=meta,
                namespace=naespace,
                name=did
            ))
            
        for dataset, files in datasets_files.items():
            self.MetacatClient.declare_files()

    def run(self):
        
        
        
        
        
        
                
        
        