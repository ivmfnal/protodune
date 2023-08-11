import sqlite3, time, sys
from logs import Logged
from pythreader import Primitive, synchronized, PyThread

class _Record(object):
    def __init__(self, filename, tstart, tend, status, info, size):
        self.Name = filename
        self.Size = size
        self.Started = tstart
        self.Ended = tend
        self.Status = status
        self.Info = info

class _HistoryDB(PyThread, Logged):

    def __init__(self, filename, keep_interval=None):
        PyThread.__init__(self, name="HistoryDB", daemon=True)
        Logged.__init__(self, name="HistoryDB")
        self.FileName = filename
        self.createTables()
        self.KeepInterval = keep_interval = keep_interval or 7*24*3600      # default: 1 week
        self.PurgeInterval = max(15, keep_interval/7)
        self.Stop = False
        
    class DBConnectionGuard(object):

        def __init__(self, filename):
            self.DBConnection = sqlite3.connect(filename)

        def __enter__(self):
            return self.DBConnection

        def __exit__(self, exc_type, exc_value, traceback):
            self.DBConnection.close()
            self.DBConnection = None
    
    def run(self):
        self.log("started")
        while not self.Stop:
            self.sleep(self.PurgeInterval)
            if not self.Stop:
                self.purgeOldRecords(time.time() - self.KeepInterval)
                self.log("Old DB records purged")
    
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
                create table if not exists file_log(
                    filename text,
                    tstart float,
                    tend float,
                    status text,
                    info text,
                    size bigint,
                    primary key (filename, tstart))
                    """)
            c.execute("""
                create index if not exists file_log_fn_event_inx on file_log(filename, status)
                """)
            c.execute("""
                create index if not exists file_log_tend_inx on file_log(tend)
                """)

    @synchronized
    def add_record(self, filename, size, tstart, tend, status, info):
        #self.debug("add_record: file:", filename, "size:", size, "start:", tstart, "end:", tend, "status", status, "info:", info)
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("""
                insert into file_log(filename, tstart, tend, status, info, size) values(?,?,?,?,?,?)
                    on conflict(filename, tstart) do update set tend = ?, status = ?
                """, (filename, tstart, tend, status, info, size,
                        tend, status
                )
            )  
            conn.commit()    
           
    def file_done(self, filename, size, tstart, tend=None):
        tend = tend or time.time()
        self.add_record(filename, size, tstart, tend, "done", "")

    def file_failed(self, filename, size, tstart, info, tend=None):
        tend = tend or time.time()
        #self.debug("file_failed:", filename, size, tstart, info, tend)
        self.add_record(filename, size, tstart, tend, "failed", info)
        
    def file_quarantined(self, filename, tstart, reason, tend=None):
        tend = tend or time.time()
        self.add_record(filename, None, tstart, tend, "quarantined", reason)
        
    @synchronized
    def latest_records_bulk(self, filenames, status=None, since=None):
        status_where = "" if not status else f" and status = '{status}' "
        since_where = "" if since is None else f" and tend >= {since}"
        fnlist = ",".join([f"'{fn}'" for fn in filenames])
        out = {}
        with self.dbconn() as conn:
            c = conn.cursor()
            sql = f"""select filename, tstart, tend, status, info, size
                    from file_log
                    where filename in ({fnlist})
                        {status_where}
                        {since_where}
                    order by tend
                """
            c.execute(sql)

            for filename, tstart, tend, status, info, size in c.fetchall():
                out[filename] = (tstart, tend, status, info, size)
        
        return out
        
    @synchronized
    def historySince(self, t=0, limit=None):
        #
        # always returns records sorted by tend in reversed order
        #
        with self.dbconn() as conn:
            c = conn.cursor()
            if limit:   limit = f"limit {limit}"
            c.execute(f"""select filename, tstart, tend, status, info, size 
                    from file_log 
                    where tend >= ?
                    order by tend desc
                    {limit}
                    """, (t,)
            )
            return [_Record(*tup) for tup in c.fetchall()]

    @synchronized
    def getRecords(self, status, since_t=None):
        since_t = since_t or 0
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("""select filename, status, tend, size, tend-tstart
                    from file_log
                    where status = ? and tend > ?
                    order by tend""",
                    (status, since_t)
            )
            return c.fetchall()

    @synchronized
    def purgeOldRecords(self, before):
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("delete from file_log where tend < ?", (before,))
            conn.commit()
            
    @synchronized
    def eventCounts(self, bin, since_t = 0):
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("""select status, tend
                    from file_log
                    where tend >= ?""", (since_t,))
            counts = {}     # (status, bin) -> count
            for status, tend in c.fetchall():
                tend = int(tend/bin)*bin
                key = (status, tend)
                counts[key] = counts.get(key, 0) + 1
            return [(key[0], key[1], count) for key, count in sorted(counts.items())]

    @synchronized
    def eventCounts____(self, bin, since_t = 0):
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("""select status, round(tend/?)*? as tt, count(*)
                    from file_log
                    where tend >= ?
                    group by status, tt
                    order by status, tt""", (bin, bin, since_t))
            return c.fetchall()

    @synchronized
    def historyForFile(self, filename):
        # returns [(t, event, info),...]
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("""
                select tstart, tend, status, info
                    from file_log 
                    where filename = ?
                    order by tend
                    """, (filename,))
            return c.fetchall()
            
def open(path):
    return _HistoryDB(path)
                
