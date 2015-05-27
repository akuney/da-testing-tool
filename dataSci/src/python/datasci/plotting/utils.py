import matplotlib
import matplotlib.pyplot as plt
import sklearn
from sklearn.metrics import roc_curve, auc
import scipy
import scipy.stats
import numpy as np


def reduce_df_row_size(df, max_points_to_plot):
    if len(df) > max_points_to_plot:
        return df.ix[np.random.choice(df.index.values, max_points_to_plot)]
    else:
        return df


def plot_scatter(df, working_dir, moniker, max_points_to_plot=10000):
    fig = plt.figure()
    r_squared = np.corrcoef(df['act'], df['pred'])[0][1] ** 2
    df_to_plot = reduce_df_row_size(df, max_points_to_plot)
    plt.scatter(df_to_plot['act'], df_to_plot['pred'], label='R^2 = %0.4f' % r_squared)
    plt.xlabel('actual values')
    plt.ylabel('predicted values')
    plt.legend(loc="lower right")
    plt.show()
    fig.savefig(working_dir + moniker + '.pdf')
    plt.close(fig)


def plot_roc_curve(df, working_dir, moniker):
    fpr, tpr, _ = sklearn.metrics.roc_curve(df['act'], df['pred'])
    roc_auc = sklearn.metrics.auc(fpr, tpr)
    fig = plt.figure()
    plt.plot(fpr, tpr, label='ROC curve (area = %0.2f)' % roc_auc)
    plt.plot([0, 1], [0, 1], 'k--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.0])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver operating characteristic')
    plt.legend(loc="lower right")
    fig.savefig(working_dir + moniker + '.pdf')
    plt.close(fig)
