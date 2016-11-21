import pandas as pd
import os
from sklearn.cluster import KMeans
from sklearn.cluster import DBSCAN
from sklearn import metrics


dataDir = os.path.join(os.path.dirname(os.path.abspath('')), 'data')

data = pd.read_csv(os.path.join(dataDir, 'Final_Merge.csv'))

print data.shape
# kmeans_model = KMeans(n_clusters=5, random_state=1)
good_columns = data._get_numeric_data()
# Fit the model using the good columns.
# kmeans_model.fit(good_columns)
# Get the cluster assignments.
# labels = kmeans_model.labels_
# print kmeans_model.n_clusters
# print kmeans_model.fit_predict(good_columns)

# Compute DBSCAN
db = DBSCAN(eps=0.3, min_samples=10).fit(good_columns)
# core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
db.fit(good_columns)
labels = db.labels_

print labels
print good_columns[labels == -1]
# Number of clusters in labels, ignoring noise if present.
n_clusters_ = len(set(labels)) - (1 if -1 in labels else 0)
print n_clusters_

# print('Estimated number of clusters: %d' % n_clusters_)
# print("Homogeneity: %0.3f" % metrics.homogeneity_score(labels_true, labels))
# print("Completeness: %0.3f" % metrics.completeness_score(labels_true, labels))
# print("V-measure: %0.3f" % metrics.v_measure_score(labels_true, labels))
# print("Adjusted Rand Index: %0.3f"
#       % metrics.adjusted_rand_score(labels_true, labels))
# print("Adjusted Mutual Information: %0.3f"
#       % metrics.adjusted_mutual_info_score(labels_true, labels))
# print("Silhouette Coefficient: %0.3f"
#       % metrics.silhouette_score(X, labels))
