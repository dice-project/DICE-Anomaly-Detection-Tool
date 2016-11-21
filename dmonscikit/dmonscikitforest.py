import numpy as np
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
import os
import pandas as pd

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

data = pd.read_csv(os.path.join(dataDir, 'Final_Merge.csv'))

# fit the model
clf = IsolationForest(max_samples='auto', verbose=1, n_jobs=-1, contamination=0.001)
clf.fit(data)
pred = clf.predict(data)
# print data.shape
# print len(pred)
anomalies = np.argwhere(pred==-1)
# print anomalies
for an in anomalies:
    print int(data.iloc[an[0]]['key'])

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