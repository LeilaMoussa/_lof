from sklearn.metrics import roc_auc_score
import matplotlib.pyplot as plt

from collections import defaultdict

def getLabels(file: str, sink=False) -> dict:
    ans = defaultdict(int)
    with open(file, "r") as _in:
        data = _in.readlines()
        for line in data:
            parts = line.strip().strip('\n').split()
            if sink:
                ans[parts[1].strip(",")] = int(parts[-1])
            else:
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

# x = {
#     # k: file
#     # later, W
#     # 3: "rtlofs/sinks/wilt-d5-ilof-k3-euc-flat-46000ms",
#     # 5: "rtlofs/sinks/wilt-d5-ilof-k5-euc-flat-80031ms",

#     # 7: "rtlofs/sinks/wilt-d5-ilof-k7-euc-flat-140328ms",
#     # 7: "rtlofs/wilt-d5-ilof-k7-euc-lsh-h4-t4-269163ms"  # same accuracy! 0.5514522741616122
#     # 7: "rtlofs/wilt-d5-ilof-k7-euc-lsh-h1-t1-257240ms"  # better 0.5570563807082878
#     # 7: "rtlofs/wilt-d5-ilof-k7-euc-lsh-h1-t6-253181ms"  # eh 0.5514522741616122
#     # 7: "rtlofs/wilt-d5-rlof-k7-euc-lsh-h1-t1-w500-i10-a2000-242154ms"  # 0.4898360655737705
#     # 7: "rtlofs/wilt-d5-rlof-k7-euc-lsh-h1-t1-w1000-i10-a2000-378522ms"  # 0.48983384346305203
#     # 7: "rtlofs/wilt-d5-rlof-k7-euc-lsh-h1-t1-w2000-i10-a2000-578325ms" # 0.5178487304471557
#     # 7: "rtlofs/wilt-d5-rlof-k7-euc-lsh-h1-t1-w2000-i20-a2000-504460ms"  # surprising: 0.5234588362457214
#     # 7: "rtlofs/wilt-d5-rlof-k7-euc-lsh-h1-t1-w2500-i10-a2000-639003ms"  # 0.5122427342948017
#     7: "rtlofs/wilt-d5-rlof-k7-euc-lsh-h1-t1-w2500-i30-a2000-472174ms"  # 0.5290605765026404

#     # 11: "rtlofs/sinks/wilt-d5-ilof-k11-euc-flat-379025ms",
#     # 13: "rtlofs/sinks/wilt-d5-ilof-k13-euc-flat-538423ms",
#     # 15: "rtlofs/sinks/wilt-d5-ilof-k15-euc-flat-760831ms",
#     # 19: "",
# }

# x = {
#     2: "rtlofs/newsinks/mouse-d2-ilof-k2-euc-flat-365ms",
#     3: "rtlofs/newsinks/mouse-d2-ilof-k3-euc-flat-450ms",
#     4: "rtlofs/newsinks/mouse-d2-ilof-k4-euc-flat-553ms",
#     5: "rtlofs/newsinks/mouse-d2-ilof-k5-euc-flat-623ms",
#     6: "rtlofs/newsinks/mouse-d2-ilof-k6-euc-flat-697ms",
#     7: "rtlofs/newsinks/mouse-d2-ilof-k7-euc-flat-880ms",
#     8: "rtlofs/newsinks/mouse-d2-ilof-k8-euc-flat-937ms",
#     9: "rtlofs/newsinks/mouse-d2-ilof-k9-euc-flat-1057ms",
#     10: "rtlofs/newsinks/mouse-d2-ilof-k10-euc-flat-1206ms",
#     11: "rtlofs/newsinks/mouse-d2-ilof-k11-euc-flat-1312ms",
#     12: "rtlofs/newsinks/mouse-d2-ilof-k12-euc-flat-1538ms",
#     13: "rtlofs/newsinks/mouse-d2-ilof-k13-euc-flat-1759ms",
#     14: "rtlofs/newsinks/mouse-d2-ilof-k14-euc-flat-1884ms",
#     15: "rtlofs/newsinks/mouse-d2-ilof-k15-euc-flat-2091ms",
#     16: "rtlofs/newsinks/mouse-d2-ilof-k16-euc-flat-2307ms",
#     17: "rtlofs/newsinks/mouse-d2-ilof-k17-euc-flat-2589ms",
#     18: "rtlofs/newsinks/mouse-d2-ilof-k18-euc-flat-3128ms",
#     19: "rtlofs/newsinks/mouse-d2-ilof-k19-euc-flat-2897ms",
#     20: "rtlofs/newsinks/mouse-d2-ilof-k20-euc-flat-3090ms",
#     21: "rtlofs/newsinks/mouse-d2-ilof-k21-euc-flat-3317ms",
#     22: "rtlofs/newsinks/mouse-d2-ilof-k22-euc-flat-3552ms",
#     23: "rtlofs/newsinks/mouse-d2-ilof-k23-euc-flat-3827ms",
#     24: "rtlofs/newsinks/mouse-d2-ilof-k24-euc-flat-4041ms",
#     25: "rtlofs/newsinks/mouse-d2-ilof-k25-euc-flat-4283ms",
# }

# x = {
#     2: "rtlofs/newsinks/wilt-d5-ilof-k2-euc-flat-33360ms",
#     3: "rtlofs/newsinks/wilt-d5-ilof-k3-euc-flat-42631ms",
#     4: "rtlofs/newsinks/wilt-d5-ilof-k4-euc-flat-59918ms",
#     5: "rtlofs/newsinks/wilt-d5-ilof-k5-euc-flat-74138ms",
#     6: "rtlofs/newsinks/wilt-d5-ilof-k6-euc-flat-106377ms",
#     7: "rtlofs/newsinks/wilt-d5-ilof-k7-euc-flat-138361ms",
#     8: "rtlofs/newsinks/wilt-d5-ilof-k8-euc-flat-231442ms",
#     9: "rtlofs/newsinks/wilt-d5-ilof-k9-euc-flat-249618ms",
#     10: "rtlofs/newsinks/wilt-d5-ilof-k10-euc-flat-316356ms",
#     11: "rtlofs/newsinks/wilt-d5-ilof-k11-euc-flat-362735ms",
#     12: "rtlofs/newsinks/wilt-d5-ilof-k12-euc-flat-535575ms",
#     13: "rtlofs/newsinks/wilt-d5-ilof-k13-euc-flat-569732ms",
#     # 14: "",
#     # 15: "",
#     # 16: "",
#     # 17: "",
#     # 18: "",
#     # 19: "",
#     # 20: "",
#     # 21: "",
#     # 22: "",
#     # 23: "",
#     # 24: "",
#     # 25: "",
# }

# x = {
#     1: "rtlofs/newsinks/mouse-d2-ilof-k7-euc-lsh-h1-t1-1532ms",
#     2: "rtlofs/newsinks/mouse-d2-ilof-k7-euc-lsh-h2-t1-1457ms",
#     3: "rtlofs/newsinks/mouse-d2-ilof-k7-euc-lsh-h3-t1-1476ms",
#     4: "rtlofs/newsinks/mouse-d2-ilof-k7-euc-lsh-h4-t1-1738ms",
#     5: "rtlofs/newsinks/mouse-d2-ilof-k7-euc-lsh-h5-t1-1707ms",
#     6: "rtlofs/newsinks/mouse-d2-ilof-k7-euc-lsh-h6-t1-1572ms",
# }

# x = {
#     1: "rtlofs/newsinks/mouse-d2-ilof-k7-euc-lsh-h2-t1-1462ms",
#     2: "rtlofs/newsinks/mouse-d2-ilof-k7-euc-lsh-h2-t2-1436ms",
#     3: "rtlofs/newsinks/mouse-d2-ilof-k7-euc-lsh-h2-t3-1524ms",
#     4: "rtlofs/newsinks/mouse-d2-ilof-k7-euc-lsh-h2-t4-1537ms",
#     5: "rtlofs/newsinks/mouse-d2-ilof-k7-euc-lsh-h2-t5-2121ms",
#     6: "rtlofs/newsinks/mouse-d2-ilof-k7-euc-lsh-h2-t6-1867ms",
# }

# x = ["rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w20-i5-a2000-1078ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w30-i5-a2000-1044ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w40-i5-a2000-1203ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w50-i5-a2000-1131ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w60-i5-a2000-1307ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w70-i5-a2000-1372ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w80-i5-a2000-1416ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w90-i5-a2000-1513ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w100-i5-a2000-1573ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w110-i5-a2000-1768ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w120-i5-a2000-1892ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w130-i5-a2000-1813ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w140-i5-a2000-1688ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w150-i5-a2000-1949ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w170-i5-a2000-2459ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w190-i5-a2000-2232ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w210-i5-a2000-2295ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w240-i5-a2000-2571ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w270-i5-a2000-2632ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w300-i5-a2000-2825ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w350-i5-a2000-2471ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w400-i5-a2000-2412ms"]

# x = [
#     "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w90-i5-a2000-1808ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w90-i10-a2000-1494ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w90-i15-a2000-1539ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w90-i20-a2000-1477ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w90-i25-a2000-1528ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w90-i30-a2000-2067ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w90-i35-a2000-1621ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w90-i40-a2000-1631ms"
# ]

# x = [
#     "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w1000-i25-a20-1523ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w1000-i25-a30-1547ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w1000-i25-a40-1476ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w1000-i25-a50-1501ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w1000-i25-a70-1472ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w1000-i25-a90-1532ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w1000-i25-a110-1505ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w1000-i25-a150-1458ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w1000-i25-a170-1550ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w1000-i25-a200-1480ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w1000-i25-a250-1518ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w1000-i25-a300-1478ms",
# "rtlofs/newsinks/mouse-d2-rlof-k7-euc-lsh-h2-t2-w1000-i25-a350-1479ms"
# ]

x = [
    "rtlofs/newsinks/immunizer-d8-rlof-k7-euc-lsh-h2-t2-w918-i25-a1530-645108ms"
]

expected_labels_file = "rtlofs/datasets/labeled.keyed.wilt"
# expected_labels_file = "rtlofs/datasets/labeled.keyed.mouse"

xax = []
yax1 = []
yax2 = []

y = getLabels(expected_labels_file)

max_auc = -1
choice = -1
#for (k, v) in x.items():
for elt in x:
    k = int(elt.split("-a")[1].split("-")[0])
    xax.append(k)
    y_pred = getLabels(elt, sink=True)
    expected, actual = diff(y, y_pred)
    auc = roc_auc_score(expected, actual)
    print(auc)
    yax1.append(auc)
    yax2.append(int(elt.split("-")[-1].strip("ms")))
    if auc > max_auc:
        max_auc = auc
        choice = k
#plt.plot(xax, yax1, label = "AUC")
plt.plot(xax, yax2, label = "ms")
plt.title("mouse-rlof-k7-euc-lsh-h2-t2-no summ for different a")
plt.xlabel("max age")
plt.ylabel("ms")
plt.legend()
plt.show()
