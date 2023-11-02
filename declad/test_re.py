import re, getopt, sys

Usage = """
python test_re.py '<pattern>' <path>
"""

opts, args = getopt.getopt(sys.argv[1:], "")

if not args:
    print(Usage)
    sys.exit(2)

pattern, path = args

pattern = re.compile(pattern)
m = pattern.match(path)
if m is None:
    print("No match")
else:
    for k in "path,type,size".split(","):
        try:
            print(k, ":", m[k])
        except IndexError:
            print(k)
