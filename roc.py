import sys
import matplotlib.pyplot as plt
from sklearn import metrics
from sklearn.metrics import RocCurveDisplay
from typing import List
import math

# NOTE: I need scikit-learn v1, which means I need Python3.7+.
# Messing with python versions on Ubuntu can get dangerous.

def getLabels(file: str, d: int) -> List[int]:
    ans = []
    # with open(file, "r") as _in:
    #     data = _in.readlines()
    #     for line in data:
    #         ans += line.split(" ")[d]
    return ans


# copied from https://scikit-learn.org/stable/auto_examples/miscellaneous/plot_outlier_detection_bench.html then modified
def plot_roc(alg_name: str, dataset_name: str, sink_file: str, expected_profiles_file: str, d: int):

    y_pred = getLabels(sink_file, d)
    y = getLabels(expected_profiles_file, d)

    datasets_name = [
        "mouse",
    ]

    models_name = [
        "RLOF",
    ]

    # plotting parameters
    cols = 1
    linewidth = 1
    pos_label = 0  # 0 belongs to positive class
    rows = math.ceil(len(datasets_name) / cols)

    fig, axs = plt.subplots(rows, cols, figsize=(10, rows * 3))
    print("axs", axs)

    # TODO use args instead of this hardcoded stuff

    for i, dataset_name in enumerate(datasets_name):
        y = [1, 2, 3]  # can't be empty, lol!

        for model_name in models_name:
            y_pred = [1.1, 2.2, 3.3]
            display = RocCurveDisplay.from_predictions(
                y,
                y_pred,
                pos_label=pos_label,
                name=model_name,
                linewidth=linewidth,
                ax=axs, # [i // cols, i % cols]
            )
        axs.plot([0, 1], [0, 1], linewidth=linewidth, linestyle=":")
        axs.set_title(dataset_name)
        axs.set_xlabel("False Positive Rate")
        axs.set_ylabel("True Positive Rate")
    plt.tight_layout(pad=2.0)
    plt.show()

if __name__ == '__main__':
    # These files contain the data labeled as 0 (inlier) or 1 (outlier)
    [_, alg_name, dataset_name, sink_file, expected_profiles_file, dim] = sys.argv
    plot_roc(alg_name, dataset_name, sink_file, expected_profiles_file, int(dim))