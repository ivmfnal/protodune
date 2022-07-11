import sqlite3, time
from pythreader import Primitive, synchronized

class _ScannerRecord(object):
    def __init__(self, server, location, t, nfiles, nnew, error):
        self.Server = server
        self.Location = location
        self.T = t
        self.NFiles = nfiles
        self.NNew = nfiles
        self.Error = error

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
            c.execute("""
                create table if not exists scanner_log(
                    server      text,
                    location    text,
                    t           float,
                    nfiles      int,
                    nnew        int,
                    error       text,
                    primary key(server, location, t)
                )
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
    def add_scanner_record(self, server, location, t, n, nnew):
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("insert into scanner_log(server, location, t, nfiles, nnew) values(?,?,?,?,?)",
                (server, location, t, n, nnew)
            )
            conn.commit()
            
    @synchronized
    def scannerHistorySince(self, t=0):
        with self.dbconn() as conn:
            c = conn.cursor()
            c.execute("""select server, location, t, nfiles, nnew, error
                    from scanner_log 
                    where tend >= ?
                    order by scanner, location, t""", (t,)
            )
            return [_ScannerRecord(*tup) for tup in c.fetchall()]

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
            c.execute("delete from scanner_log where t < ?", (before,))
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
            
def open(path):
    return _HistoryDB(path)
                
