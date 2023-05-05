from metacat.filters import MetaCatFilter
from wsdbtools import ConnectionPool
import re
from condb import ConDB

class RunsDBinConDB(MetaCatFilter):
    """
    Inputs:

    Positional parameters: none

    Keyword parameters:

    Description:

    Configuration:
        host =
        port =
        dbname =
        user =
        password =
    """

    def __init__ (self, config):
        self.Config = config
        show_config = config.copy()
        show_config["connection"] = self.hide(show_config["connection"], "user", "password")
        MetaCatFilter.__init__(self, show_config)
        self.Connection = self.Config["connection"]
        self.ConnPool = ConnectionPool(postgres=self.Connection, max_idle_connections=1)
        self.FolderName = self.Config["folder"]
        self.MetaPrefix = self.Config.get("meta_prefix", "runs_history")
        
        #
        # get column names
        #
        
        db = ConDB(self.Connection)
        folder = db.openFolder(self.FolderName)
        folder_columns = folder.data_column_types()
        self.Columns = list(zip(*folder_columns))[0]

    def hide(self, conn, *fields):
        for f in fields:
             conn = re.sub(f"\s+{f}\s*=\s*\S+", f" {f}=(hidden)", conn, re.I)
        return conn

    def filter(self, inputs, **ignore):
        
        # Conect to db via condb python API
        db = ConDB(self.Connection)
        folder = db.openFolder(self.FolderName)

        # Get files from metacat input
        file_set = inputs[0]
        for chunk in file_set.chunked():
            run_nums = set()

            for f in chunk:
                file_runs = f.Metadata.get("core.runs")
                if file_runs:
                    runnum = file_runs[0]
                    run_nums.add(runnum)

            if run_nums:
                # Get run_hist data
                data_runhist = folder.getData(0, channel_range=(min(run_nums), max(run_nums)+1))
                data_by_run = {row[0]: row[4:] for row in data_runhist}     # skip channel, tv, data_type and tr
        
                # Insert run hist data to Metacat
                for f in chunk:
                    file_runs = f.Metadata.get("core.runs")
                    if file_runs:
                        runnum = file_runs[0]
                        if runnum in data_by_run:
                            for col, value in zip(self.Columns, data_by_run[runnum]):
                                f.Metadata[f"{self.MetaPrefix}.{col}"] = value

            yield from chunk
 

def create_filters():
    return {
        "dune_runshistdb": RunsDBinConDB()
    }
