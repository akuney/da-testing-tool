import pandas as pd
import numpy as np
import sklearn
import sklearn.cross_validation


class KFoldLabel(object):

    """K-Fold Label cross validation iterator.

    Provides train/test indices to split data in train test sets.  Split
    dataset into k consecutive folds (based on labels).

    NOTE: this should exist in sklearn but does not!
    sklearn.cross_validation.KFold is very similar but does not pay attention
    to labels.
    sklearn.cross_validation.LeavePLabelOut is similar but creates a huge
    number of cross validation splits.


    Parameters
    ----------
    labels : array-like of int with shape (n_samples,)
        Arbitrary domain-specific stratification of the data to be used
        to draw the splits.
    n_folds : int, default=3
        Number of folds.
    indices : boolean, optional (default True)
    shuffle : boolean, optional
        Whether to shuffle the data before splitting into batches.
    random_state : int or RandomState
        Pseudo number generator state used for random sampling.
    """
    def __init__(self, labels, n_folds=3, indices=True, shuffle=False,
                random_state=None):
        self.labels = np.array(labels, copy=True)
        self.unique_labels = np.unique(labels)
        self.n_unique_labels = len(self.unique_labels)
        self.idxs = np.arange(self.n_unique_labels)
        self.n_folds = n_folds
        self.indices = indices
        self.shuffle = shuffle
        self.random_state = random_state

    def __iter__(self):
        n_unique_labels = self.n_unique_labels
        n_folds = self.n_folds
        fold_sizes = (n_unique_labels // n_folds) *\
                     np.ones(n_folds, dtype=np.int)
        fold_sizes[:n_unique_labels % n_folds] += 1
        current = 0
        if self.indices:
            ind = np.arange(len(self.labels))
        for fold_size in fold_sizes:
            test_index = np.ones(len(self.labels), dtype=np.bool)
            start, stop = current, current + fold_size
            for l in self.unique_labels[start:stop]:
                test_index[self.labels == l] = False
            train_index = np.logical_not(test_index)
            if self.indices:
                train_index = ind[train_index]
                test_index = ind[test_index]
            current = stop
            yield train_index, test_index

    def __repr__(self):
        return '%s.%s(labels=%s, p=%s)' % (
            self.__class__.__module__,
            self.__class__.__name__,
            self.labels,
            self.n_folds,
        )

    def __len__(self):
        return self.n_folds

