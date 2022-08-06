import os
import sys, getopt

Usage = """
python make_files.py [-m <metadata_file.json>] [-s <size, MB>] <file_pattern_[i].data> <i0> <n> <outdir> 
"""

opts, args = getopt.getopt(sys.argv[1:], "m:")

metadata = None
size = 1
for opt, val in opts:
    if opt == "-m": metadata = open(val, "r").read()
    if opt == "-s": size = int(val)
    
pattern = args[0]
i0 = int(args[1])
n = int(args[2])
outdir = args[3]

MB = "_"*1024*2014

for i in range(i0, i0+n):
    fn = outdir + "/" + pattern.replace("[i]", "%d" % (i,))
    print("writing %s ..." % (fn,))
    f = open(fn, "wb")
    for s in range(size):
        f.write(MB)
    f.close()
    if metadata:
        print("writing metadata ...")
        open(fn + ".json", "w").write(metadata)

    
