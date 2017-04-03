import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
import os
import pandas as pd
import scipy
from scipy import stats

rng = np.random.RandomState(42)

# # Generate train data
# X = 0.3 * rng.randn(100, 2)
# X_train = np.r_[X + 2, X - 2]
# # Generate some regular novel observations
# X = 0.3 * rng.randn(20, 2)
# X_test = np.r_[X + 2, X - 2]
# # Generate some abnormal novel observations
# X_outliers = rng.uniform(low=-4, high=4, size=(20, 2))


dataDir = os.path.join(os.path.dirname(os.path.abspath('')), 'data')

data = pd.read_csv(os.path.join(dataDir, 'Storm.csv'))

# difference
# print data.set_index('key').diff()

# distan measure %
# distance = lambda column1, column2: (column1 - column2).abs().sum() / len(column1)
# result = data.apply(lambda col1: data.apply(lambda col2: distance(col1, col2)))
# print result.head()

# correlation coefficient
# distance_cor = lambda column1, column2: scipy.stats.pearsonr(column1, column2)[0]
# result_corr = data.apply(lambda col1: data.apply(lambda col2: distance_cor(col1, col2)))
# print result_corr.head()


# Euclidian Distance
# distance_euc = lambda column1, column2: pd.np.linalg.norm(column1 - column2)
# result_euc = data.apply(lambda col1: data.apply(lambda col2: distance_euc(col1, col2)))
# print result_euc.head()

# fit the model
clf = IsolationForest(max_samples='auto', verbose=1, n_jobs=-1, contamination=0.1)
clf.fit(data)
pred = clf.predict(data)
# print data.shape
# print len(pred)
# print pred
anomalies = np.argwhere(pred == -1)
normal = np.argwhere(pred == 1)
# print anomalies
# print normal

test = pd.DataFrame()
slist = []
for an in anomalies:
    print int(data.iloc[an[0]]['key'])
    slist.append(data.iloc[an[0]])
    # print data.iloc[an[0]]
    # print data.iloc[an[0]]



test = pd.DataFrame(slist)
# for s in slist:
#     test.append(slist, ignore_index=False)
test.set_index('key', inplace=True)
print test.index
# print 'done'
print type(test)


for anomaly in anomalies:
    chkValues = []
    for event in normal:
        chkValues.append(data.iloc[event[0]])
    chkValues.append(data.iloc[anomaly[0]])
    chkDF = pd.DataFrame(chkValues)
    chkDF.set_index('key', inplace=True)
    # print chkDF.index
    # print chkDF
    # cause = chkDF[chkDF.diff()!=0].stack()
    # print cause
    print "&" *100
    distance = lambda column1, column2: (column1 - column2).abs().sum() / len(column1)
    result = chkDF.apply(lambda col1: chkDF.apply(lambda col2: distance(col1, col2)))
    # print result.head()
    # print type(result)
    print result.to_dict()
    print "&" * 100

# print pred.w
# for el in pred:
#     if el == -1:
#         print "found anomaly"
#         print type(el)



# y_pred_train = clf.predict(X_train)
# y_pred_test = clf.predict(X_test)
# y_pred_outliers = clf.predict(X_outliers)
# print y_pred_train
# print y_pred_test
# print y_pred_outliers




# plot the line, the samples, and the nearest vectors to the plane
# xx, yy = np.meshgrid(np.linspace(-5, 5, 50), np.linspace(-5, 5, 50))
# Z = clf.decision_function(np.c_[xx.ravel(), yy.ravel()])
# Z = Z.reshape(xx.shape)

# plt.title("IsolationForest")
# plt.contourf(xx, yy, Z, cmap=plt.cm.Blues_r)
#
# b1 = plt.scatter(X_train[:, 0], X_train[:, 1], c='white')
# b2 = plt.scatter(X_test[:, 0], X_test[:, 1], c='green')
# c = plt.scatter(X_outliers[:, 0], X_outliers[:, 1], c='red')
# plt.axis('tight')
# plt.xlim((-5, 5))
# plt.ylim((-5, 5))
# plt.legend([b1, b2, c],
#            ["training observations",
#             "new regular observations", "new abnormal observations"],
#            loc="upper left")
# plt.show()