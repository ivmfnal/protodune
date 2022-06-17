import subprocess, sys, os, time, glob, stat, fnmatch, traceback, getopt

Usage = """
python rmfiles.py [-d] [-v] <server>[:<port>] <directory> ["<pattern>"]
"""

Debug = False
Verbose = False
Pattern = "*"


def debug(msg):
    if Debug:
        print msg

def runCommand(cmd):
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    out, _ = p.communicate()
    debug("%s %s\n%s\n" % (cmd, p.returncode, out))
    return p.returncode, out

opts, args = getopt.getopt(sys.argv[1:], "vd")

for opt, val in opts:
    if opt == '-v': Verbose = True
    if opt == '-d': Debug = True
    
Server = args[0]
Directory = args[1]
Pattern = args[2]

command = "xrdfs %s ls %s" % (Server, Directory)
status, out = runCommand(command)
if status:
    debug("Error in ls command: %d" % (status,))
    print out
    sys.exit(1)
    
files = [x.strip() for x in out.split("\n")]

for path in files:
    if path.startswith(Directory + '/'):
        fn = path[len(Directory)+1:]
        if fnmatch.fnmatch(fn, Pattern):
            command = "xrdfs %s rm %s" % (Server, path)
            if Verbose:
                print "deleting %s ..." % (path,)
            status, out = runCommand(command)
            if status:
                print "Error deleting file %s: %d %s" % (path, status, out)
            

    
    
