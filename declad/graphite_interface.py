import pickle, time, struct, socket
from tools import to_str, to_bytes
from pythreader import PyThread
from logs import Logged

def sanitize_key(key):
    if key is None:
        return key
    replacements = {
            ".": "_",
            " ": "_",
    }
    for old,new in replacements.items():
        key = key.replace(old, new)
    return key

class GraphiteInterface:
    def __init__(self, host, port, namespace, debug=False):
        # pickle only for now
        self.host = host
        self.pickle_port = port
        self.namespace = namespace
        assert self.host and self.pickle_port and self.namespace
        self.Debug = debug
        self.AccumulatedData = []
        self.BatchSize = 1000
        
    def debug(self, msg):
        if self.Debug:
            print(("GraphiteInterface: %s" % (msg,)))
        
    def errorLog(self, msg):
        pass

    def send_dict(self, data, batch_size=1000):
        """send data contained in dictionary as {k: v} to graphite dataset
        $namespace.k with current timestamp"""
        timestamp=time.time()
        post_data=[]
        # turning data dict into [('$path.$key',($timestamp,$value)),...]]
        for k,v in data.items():
            t = (self.namespace+"."+k, (timestamp, v))
            post_data.append(t)
            #logger.debug(str(t))
        return self.post_formatted_data(post_data, batch_size)
            
    def flatten_dict(self, dct, key_base):
        dct_out = {}
        for k, v in dct.items():
            key = key_base + "." + k
            if type(v) == type({}):
                dct_out.update(self.flatten_dict(v,key))
            else:
                dct_out[key] = v
        return dct_out
                
    def send_timed_array(self, lst, batch_size=1000):
        # lst is a list of tuples:
        # [(timestamp, dct),...]
        # dct can be nested dictionary with data. each key branch will be represented as dot-separated string
        data_list = []
        for t, dct in lst:
            t = float(t)
            dct = self.flatten_dict(dct, self.namespace)
            data_list += [(k, (t, v)) for k, v in dct.items()]
        #for k, (t, v) in data_list:
        #    print t, k, v
        return self.post_formatted_data(data_list, batch_size)
        
    def feedData(self, t, path, value):
        self.AccumulatedData.append((self.namespace + "." + path, (float(t), value)))
        if len(self.AccumulatedData) > self.BatchSize:
            self.flushData()
            
    def flushData(self):
        self.post_formatted_data(self.AccumulatedData)
        #print "sent data: ", len(self.AccumulatedData)
        #print self.AccumulatedData[:5]
        self.AccumulatedData = []
        
        
    def post_formatted_data(self, post_data, batch_size=1000):
        #
        # post_data: [(key, (t, val)),...]
        for i in range(len(post_data)//batch_size + 1):
            # pickle data
            batch = post_data[i*batch_size:(i+1)*batch_size]
            payload = to_bytes(pickle.dumps(batch, protocol=2))
            header = struct.pack("!L", len(payload))
            message = header + payload
            # throw data at graphite
            s=socket.socket()
            try:
                s.connect( (self.host, self.pickle_port) )
                s.sendall(message)
                #self.debug("sent to %s:%s: %s" % (self.host, self.pickle_port, batch))
            except socket.error as e:
                self.errorLog("unable to send data to graphite at %s:%d" % (self.host,self.pickle_port))
                #print "unable to send data to graphite at %s:%d\n" % (self.host,self.pickle_port)
            finally:
                s.close()


class GraphiteSender(PyThread, Logged):

    Events = ("done", "quarantined", "failed")

    def __init__(self, config, history_db):
        Logged.__init__(self, "GraphiteSender")
        self.GraphiteHost = config["host"]
        self.GraphitePort = config["port"]
        self.Namespace = config["namespace"]
        self.Interval = config.get("interval", 30)
        self.Bin = config.get("bin", 60)
        self.GInterface = GraphiteInterface(self.GraphiteHost, self.GraphitePort, self.Namespace)
        self.HistoryDB = history_db
        PyThread.__init__(self, daemon=True)
        self.Stop = False
        
    def run(self):
        while not self.Stop:
            self.sleep(self.Interval)
            if not self.Stop:
                t0 = int(time.time() - self.Interval*10)/self.Bin*self.Bin
                counts = self.HistoryDB.eventCounts(self.Bin, t0)
                out = {}        # { t: {event:count}, ... }
                totals = dict([(e,0) for e in self.Events])
                for event, t, count in counts:
                    t = t + self.Bin/2
                    if event in self.Events:
                        if not t in out:
                            out[t] = {e: 0.0 for e in self.Events}
                        #print "graphite: event: %s, t: %s, n: %d" % (event, t, count)
                        out[t][event] = float(count)/self.Bin
                        totals[event] += count
                #self.log("Stats collected:")
                #for event, count in sorted(totals.items()):
                #    self.log(f"    {event}: {count}")
                self.GInterface.send_timed_array(sorted(out.items()))
