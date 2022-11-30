# A very specific script that simply prepends a key (id) to each data point.
def do():
    with open('rtlofs/wilt.labeled', 'r') as unkeyed:
        key = 1
        with open('rtlofs/labeled.keyed.wilt', 'a') as keyed:
            while True:
                line = unkeyed.readline()
                if not line:
                    break
                feats = line.split()
                out = str(key) + " " + ' '.join(feats[:-2] + feats[-1:]) + '\n'  # specific to wilt
                keyed.write(out)
                key += 1

if __name__ == '__main__':
    do()
