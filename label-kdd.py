# A very specific script to keep only selected anomalies from KDD9910pc and label data as 0 or 1
def do():
    with open("../datasets/kddcup.data_10_percent_corrected", "r") as _in:
        inlier = 0
        outlier = 0
        allowed = ["pod", "guess_passwd", 
        "land", "spy", "phf", 
        "warezmaster", "rootkit", 
        "multihop", "loadmodule", 
        "ftp_write", "buffer_overflow",
        "imap", "back"]
        with open("rtlofs/sampled.labeled.kddcup.data_10_percent", "a") as _out:
            while True:
                line = _in.readline()
                if not line:
                    break
                feats = line.strip('.\n').split(",")
                if feats[41] == "normal":
                    label = '0'
                    inlier += 1
                elif feats[41] in allowed:
                    label = '1'
                    outlier += 1
                else:
                    continue
                out = ' '.join(feats[0:41] + [label])
                _out.write(out + '\n')
        print(inlier, outlier)  # Total: 99921, inliers: 97278, outliers: 2643

if __name__ == '__main__':
    do()
