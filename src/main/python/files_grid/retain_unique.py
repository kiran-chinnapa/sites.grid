import sys

prev_line = ""
with open(sys.argv[2],'w') as out:
    with open(sys.argv[1]) as file:
        for line in file:
            if prev_line != line:
                out.write(line)
                prev_line = line
