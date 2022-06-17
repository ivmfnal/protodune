import sys, zlib, os, json

Usage = """
python adler32.py <data file> [<json metadata file>]
"""

if len(sys.argv[1:]) not in (1,2):
    print(Usage)
    sys.exit(2)

data_file = sys.argv[1]
data_file = open(data_file, "rb")

metadata = {}
meta_file = None
if len(sys.argv[1:]) == 2:
    meta_file = sys.argv[2]
    if os.path.isfile(meta_file):
        metadata = json.load(open(meta_file, "r"))

checksum = zlib.adler32(b"")
size = 0
block = data_file.read(16*1024)
while block:
    size += len(block)
    checksum = zlib.adler32(block, checksum)
    block = data_file.read(16*1024)

checksum = "%08x" % (checksum,)
print(checksum, size)

if meta_file:
    metadata["checksum"] = checksum
    metadata["file_size"] = size
    json.dump(metadata, open(meta_file, "w"))
    
