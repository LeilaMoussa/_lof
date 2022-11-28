# A very specific script that simply prepends a key (id) to each data point from mouse.
def do():
    with open('rtlofs/labeled-mouse.txt', 'r') as unkeyed:
        key = 1
        with open('rtlofs/labeled.keyed.mouse', 'a') as keyed:
            while True:
                line = unkeyed.readline()
                if not line:
                    break
                out = str(key) + " " + line  # line is attrs + label
                keyed.write(out)
                key += 1

if __name__ == '__main__':
    do()
