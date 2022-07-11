import time
import os
import datetime
from pythreader import PyThread, synchronized, Primitive
from threading import Timer, Thread

def make_timestamp(t=None):
    if t is None:   
        t = datetime.datetime.now()
    elif isinstance(t, (int, float)):
        t = datetime.datetime.fromtimestamp(t)
    return t.strftime("%m/%d/%Y %H:%M:%S") + ".%03d" % (t.microsecond//1000)
                
class LogStream(Primitive):

    def __init__(self, stream, **ignore):
        Primitive.__init__(self, name=f"LogStream({stream})")
        self.Stream = stream            # sys.stdout, sys.stderr

    @synchronized
    def log(self, msg, raw=False, t=None):
        if t != False and not raw:
            msg = "%s: %s" % (make_timestamp(t), msg)
        self.write(msg + '\n');

    @synchronized
    def write(self, msg):
        self.Stream.write(msg);
        self.Stream.flush()
        
class LogFile(Primitive):
    
        def __init__(self, path, interval = '1d', keep = 10, add_timestamp=True, append=True, flush_interval=None, name=None):
            # interval = 'midnight' means roll over at midnight
            Primitive.__init__(self, name=f"LogFile({path})")
            self.File = None
            assert isinstance(path, str), "LogFile.__init__: path must be a string. Got %s %s instead" % (type(path), path) 
            self.Path = path
            self.CurLogBegin = 0
            if type(interval) == type(''):
                    mult = 1
                    if interval[-1] == 'd' or interval[-1] == 'D':
                            interval = interval[:-1]
                            mult = 24 * 3600
                            interval = int(interval) * mult
                    elif interval[-1] == 'h' or interval[-1] == 'H':
                            interval = interval[:-1]
                            mult = 3600
                            interval = int(interval) * mult
                    elif interval[-1] == 'm' or interval[-1] == 'M':
                            interval = interval[:-1]
                            mult = 60
                            interval = int(interval) * mult
            self.Interval = interval
            self.Keep = keep
            self.AddTimestamps = add_timestamp
            self.LineBuf = ''
            self.LastLog = None
            self.LastFlush = time.time()
            append = append and os.path.isfile(self.Path)
            if append:
                self.File = open(self.Path, 'a')
                self.File.write("%s: --- log reopened ---\n" % (make_timestamp(),))
                self.CurLogBegin = time.time()
            else:
                self.newLog()
            #print("LogFile: created with file:", self.File)
            if flush_interval is not None:
                self.arm_flush_timer(flush_interval)
                
        def newLog(self):
            if self.File != None:
                    self.File.close()
            try:    os.remove('%s.%d' % (self.Path, self.Keep))
            except: pass
            for i in range(self.Keep - 1):
                    inx = self.Keep - i
                    old = '%s.%d' % (self.Path, inx - 1)
                    new = '%s.%d' % (self.Path, inx)
                    try:    os.rename(old, new)
                    except: pass
            try:    os.rename(self.Path, self.Path + '.1')
            except: pass
            self.File = open(self.Path, 'w')
            self.CurLogBegin = time.time()

        @synchronized
        def log(self, msg, raw=False, t=None):
            if t is None:   t = time.time()
            if self.Interval == 'midnight':
                if datetime.date.today() != self.LastLog:
                        self.newLog()
            elif isinstance(self.Interval, (int, float)):
                if t > self.CurLogBegin + self.Interval:
                    self.newLog()
            if t != False and not raw:
                msg = "%s: %s" % (make_timestamp(t), msg)
            self._write(msg if raw else msg + "\n")

        @synchronized
        def write(self, msg):
            self.log(msg, raw=True)
            
        @synchronized
        def _write(self, msg):
            if msg:
                #print("LogFile.write: writing to:", self.File)
                self.File.write(msg)
            self.flush()
            self.LastLog = datetime.date.today()

        def arm_flush_timer(self, interval):
            if interval:
                Timer(interval, self.flush).start()
                    
        @synchronized
        def flush(self, interval=None):
            if self.File is not None:
                self.File.flush()
            if interval:
                self.arm_flush_timer(interval)
                
        def start(self):
            # for compatibility with clients, which think LogFile is a thread
            if isinstance(self, PyThread):
                PyThread.start(self)
            elif isinstance(self, Thread):
                Thread.start(self)
            else:
                pass
                
        def __del__(self):
            if self.File is not None:
                self.File.close()
                self.File = None


