# A very specific script that simply prepends a key (id) to each data point.
def do():
    with open('rtlofs/shuttle.labeled', 'r') as unkeyed:
        key = 1
        with open('rtlofs/labeled.keyed.shuttle', 'a') as keyed:
            while True:
                line = unkeyed.readline()
                if not line:
                    break
                out = str(key) + " " + line  # line is attrs + label
                keyed.write(out)
                key += 1

if __name__ == '__main__':
    do()
