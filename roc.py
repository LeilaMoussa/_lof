import sys
import matplotlib.pyplot as plt
from sklearn.metrics import RocCurveDisplay
from typing import List
import math
from sklearn.metrics import PrecisionRecallDisplay

def getLabels(file: str, d: int) -> List[int]:
    ans = []
    with open(file, "r") as _in:
        data = _in.readlines()
        data.sort() # to get same order based on ids
        for line in data:
            ans.append(int(line.strip().strip('\n').split()[-1]))  # append label
    return ans

# copied from https://scikit-learn.org/stable/auto_examples/miscellaneous/plot_outlier_detection_bench.html then modified
def plot_roc(alg_name: str, dataset_name: str, sink_file: str, expected_profiles_file: str, d: int):

    datasets_name = [
        dataset_name,
    ]

    models_name = [
        alg_name,
    ]

    # plotting parameters
    cols = 1
    linewidth = 1
    pos_label = 1  # 1 means outlier
    rows = math.ceil(len(datasets_name) / cols)

    fig, axs = plt.subplots(rows, cols, figsize=(10, rows * 3))

    for i, dataset in enumerate(datasets_name):
        y = getLabels(expected_profiles_file, d)

        for model_name in models_name:
            y_pred = getLabels(sink_file, d)
            display = RocCurveDisplay.from_predictions(
                y,
                y_pred,
                pos_label=pos_label,
                name=model_name,
                linewidth=linewidth,
                ax=axs, # [i // cols, i % cols]
            )

            # Might want to do Precision-Recall curve instead
            # display = PrecisionRecallDisplay.from_predictions(y, y_pred, name=model_name) _ = display.ax_.set_title("Outlier Detection Precision-Recall Curve")
        axs.plot([0, 1], [0, 1], linewidth=linewidth, linestyle=":")
        axs.set_title(dataset)
        axs.set_xlabel("False Positive Rate")
        axs.set_ylabel("True Positive Rate")
    plt.tight_layout(pad=2.0)
    plt.show()

if __name__ == '__main__':
    # These files contain the data labeled as 0 (inlier) or 1 (outlier) with the same ids
    # python roc.py ILOF mouse ilof.txt original.txt 2
    # python roc.py RLOF mouse rlof.txt original.txt 2
    # python roc.py RLOF ilof-mouse rlof.txt ilof.txt 2
    [_, alg_name, dataset_name, sink_file, expected_profiles_file, dim] = sys.argv
    plot_roc(alg_name, dataset_name, sink_file, expected_profiles_file, int(dim))
