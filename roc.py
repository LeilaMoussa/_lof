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

    #fig, axs = plt.subplots(1, 1, figsize=(10, 3))  # TODO: one fixed plot instead of subplots

    y_pred = getLabels(sink_file, d)
    y = getLabels(expected_profiles_file, d)

    # fpr, tpr, thresholds = metrics.roc_curve(y, y_pred, pos_label=0)
    # roc_auc = metrics.auc(fpr, tpr)
    # display = metrics.RocCurveDisplay(fpr=fpr, tpr=tpr, roc_auc=roc_auc, estimator_name='example estimator')
    # display.plot()  
    # plt.show()



    # linewidth = 1

    # i = 0
    # cols = 1  # TODO: remove these, just make sure axes work fine

    # display = RocCurveDisplay.from_predictions(
    #     y,
    #     y_pred,
    #     pos_label=0, # mean 0 belongs to positive class
    #     name=alg_name,
    #     linewidth=linewidth,
    #     # ax=axs[i // cols, i % cols],
    # )

    # # axs[i // cols, i % cols].plot([0, 1], [0, 1], linewidth=linewidth, linestyle=":")
    # # axs[i // cols, i % cols].set_title(dataset_name)
    # # axs[i // cols, i % cols].set_xlabel("False Positive Rate")
    # # axs[i // cols, i % cols].set_ylabel("True Positive Rate")

    # plt.tight_layout(pad=2.0)  # spacing between subplots  # TODO i don't need this?
    # plt.show()

    datasets_name = [
    "http",
    "smtp",
    "SA",
    "SF",
    "forestcover",
    "glass",
    "wdbc",
    "cardiotocography",
    ]

    models_name = [
        "LOF",
        "IForest",
    ]

    # plotting parameters
    cols = 2
    linewidth = 1
    pos_label = 0  # mean 0 belongs to positive class
    rows = math.ceil(len(datasets_name) / cols)

    fig, axs = plt.subplots(rows, cols, figsize=(10, rows * 3))

    for i, dataset_name in enumerate(datasets_name):
        y = []  # !!

        for model_name in models_name:
            y_pred = []  # !!
            display = RocCurveDisplay.from_predictions(  # IndexError: cannot do a non-empty take from an empty axes.
                y,
                y_pred,
                pos_label=pos_label,
                name=model_name,
                linewidth=linewidth,
                ax=axs[i // cols, i % cols],
            )
        axs[i // cols, i % cols].plot([0, 1], [0, 1], linewidth=linewidth, linestyle=":")
        axs[i // cols, i % cols].set_title(dataset_name)
        axs[i // cols, i % cols].set_xlabel("False Positive Rate")
        axs[i // cols, i % cols].set_ylabel("True Positive Rate")
    plt.tight_layout(pad=2.0)  # spacing between subplots
    plt.show()


if __name__ == '__main__':
    # These files contain the data labeled as 0 (inlier) or 1 (outlier)
    [_, alg_name, dataset_name, sink_file, expected_profiles_file, dim] = sys.argv
    plot_roc(alg_name, dataset_name, sink_file, expected_profiles_file, int(dim))
