import pandas as pd
from sklearn.feature_selection import VarianceThreshold
import matplotlib.pyplot as plt
import numpy as np
import os

# dataDir = os.path.join(os.path.dirname(os.path.abspath('')), 'data')
dataDir = "/Users/Gabriel/Documents/workspaces/diceWorkspace/dmon-adp/data"
corrFile = os.path.join(dataDir, 'Spearman.csv')
print dataDir
data = os.path.join(dataDir, 'Final_Merge.csv')
data_df = pd.read_csv(data)
print data_df.shape
data_df.drop(['key'], axis=1, inplace=True)
# sel = VarianceThreshold(threshold=(.8 * (1 - .8)))
# print type(sel.fit_transform(data_df))
# print sel.fit_transform(data_df).shape
# print data_df.shape
# print type(data_df)


def varianceSelection(X, THRESHOLD = .8):
    sel = VarianceThreshold(threshold=(THRESHOLD * (1 - THRESHOLD)))
    sel.fit_transform(X)
    return X[[c for (s, c) in zip(sel.get_support(), X.columns.values) if s]]


test = varianceSelection(data_df, THRESHOLD=.95)
print test.shape
print type(test)

print test


corr = test.corr(method='spearman', min_periods=1)
# corr.to_csv(corrFile)
# print corr
print set(data_df.columns.values) - set(test.columns.values)


# show image using matplotlib
plt.pcolor(corr)
plt.yticks(np.arange(0.5, len(corr.index), 1),  corr.index)
plt.xticks(np.arange(0.5, len(corr.columns), 1), corr.columns)
plt.show()