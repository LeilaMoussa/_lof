import sys
import matplotlib.pyplot as plt
from sklearn.metrics import RocCurveDisplay
from typing import List
import math
from sklearn.metrics import PrecisionRecallDisplay
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

# copied from https://scikit-learn.org/stable/auto_examples/miscellaneous/plot_outlier_detection_bench.html then modified
def plot_roc(alg_name: str, dataset_name: str, sink_file: str, expected_profiles_file: str):

    datasets_name = [
        dataset_name,
    ]

    models_name = [
        alg_name,
    ]

    cols = 1
    linewidth = 1
    pos_label = 1  # 1 means outlier
    rows = math.ceil(len(datasets_name) / cols)

    fig, axs = plt.subplots(rows, cols, figsize=(10, rows * 3))

    for i, dataset in enumerate(datasets_name):
        y = getLabels(expected_profiles_file)

        for model_name in models_name:
            y_pred = getLabels(sink_file)
            expected, actual = diff(y, y_pred)
            RocCurveDisplay.from_predictions(
                expected,
                actual,
                pos_label=pos_label,
                name=model_name,
                linewidth=linewidth,
                ax=axs, # [i // cols, i % cols]
            )

        axs.plot([0, 1], [0, 1], linewidth=linewidth, linestyle=":")
        axs.set_title(dataset)
        axs.set_xlabel("False Positive Rate")
        axs.set_ylabel("True Positive Rate")
    plt.tight_layout(pad=2.0)
    plt.show()

if __name__ == '__main__':
    # These files contain the data labeled as 0 (inlier) or 1 (outlier) with the same ids
    # python roc.py ilof-flat-k10 wilt actual.txt expected.txt
    [_, alg_name, dataset_name, sink_file, expected_profiles_file] = sys.argv
    plot_roc(alg_name, dataset_name, sink_file, expected_profiles_file)
