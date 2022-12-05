from sklearn.metrics import roc_auc_score

from collections import defaultdict

def getLabels(file: str) -> dict:
    ans = defaultdict(int)
    with open(file, "r") as _in:
        data = _in.readlines()
        for line in data:
            parts = line.strip().strip('\n').split()
            ans[parts[0]] = int(parts[-1])
    return ans

def diff(ex: dict, act: dict):
    expected = []
    actual = []
    for elt in ex.keys():
        if elt in act:
            expected.append(ex[elt])
            actual.append(act[elt])
    return expected, actual

x = {
    # k: file
    # later, W
    # 3: "rtlofs/sinks/wilt-d5-ilof-k3-euc-flat-46000ms",
    # 5: "rtlofs/sinks/wilt-d5-ilof-k5-euc-flat-80031ms",

    # 7: "rtlofs/sinks/wilt-d5-ilof-k7-euc-flat-140328ms",
    # 7: "rtlofs/wilt-d5-ilof-k7-euc-lsh-h4-t4-269163ms"  # same accuracy! 0.5514522741616122
    # 7: "rtlofs/wilt-d5-ilof-k7-euc-lsh-h1-t1-257240ms"  # better 0.5570563807082878
    # 7: "rtlofs/wilt-d5-ilof-k7-euc-lsh-h1-t6-253181ms"  # eh 0.5514522741616122
    # 7: "rtlofs/wilt-d5-rlof-k7-euc-lsh-h1-t1-w500-i10-a2000-242154ms"  # 0.4898360655737705
    # 7: "rtlofs/wilt-d5-rlof-k7-euc-lsh-h1-t1-w1000-i10-a2000-378522ms"  # 0.48983384346305203
    # 7: "rtlofs/wilt-d5-rlof-k7-euc-lsh-h1-t1-w2000-i10-a2000-578325ms" # 0.5178487304471557
    # 7: "rtlofs/wilt-d5-rlof-k7-euc-lsh-h1-t1-w2000-i20-a2000-504460ms"  # surprising: 0.5234588362457214
    # 7: "rtlofs/wilt-d5-rlof-k7-euc-lsh-h1-t1-w2500-i10-a2000-639003ms"  # 0.5122427342948017
    7: "rtlofs/wilt-d5-rlof-k7-euc-lsh-h1-t1-w2500-i30-a2000-472174ms"  # 0.5290605765026404

    # 11: "rtlofs/sinks/wilt-d5-ilof-k11-euc-flat-379025ms",
    # 13: "rtlofs/sinks/wilt-d5-ilof-k13-euc-flat-538423ms",
    # 15: "rtlofs/sinks/wilt-d5-ilof-k15-euc-flat-760831ms",
    # 19: "",
}

expected_labels_file = "rtlofs/datasets/labeled.keyed.wilt"

y = getLabels(expected_labels_file)

max_auc = -1
choice = -1
for (k, v) in x.items():
    y_pred = getLabels(v)
    expected, actual = diff(y, y_pred)
    auc = roc_auc_score(expected, actual)
    print(k, auc)
    # 3 0.5458481676149366
    # 5 0.5346399545215853
    # 7 0.5514522741616122
    # 11 0.5402440610682611
    # 13 0.5346399545215853
    # 15 0.5346399545215853
    if auc > max_auc:
        max_auc = auc
        choice = k
print("best param", choice, "with auc", max_auc)  # wilt ilog flat euc k7 auc0.5514522741616122
