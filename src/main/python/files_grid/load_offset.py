import sys

with open(sys.argv[1]) as in_file, open('.offset','w') as out_file:
    out_file.write(str(in_file.tell()))
    line = in_file.readline()
    while line:
        out_file.write(":" + str(in_file.tell()))
        line = in_file.readline()