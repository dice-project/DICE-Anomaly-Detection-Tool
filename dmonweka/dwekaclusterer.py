import os
import tempfile
import traceback
import weka.core.jvm as jvm
import dmonweka.helper as helper
from weka.clusterers import Clusterer
import weka.core.converters as converters
import weka.core.serialization as serialization
from dataformatter import DataFormatter
import weka.core.packages as packages


dataDir = os.path.join(os.path.dirname(os.path.abspath('')), 'data')
modelDir = os.path.join(os.path.dirname(os.path.abspath('')), 'models')




dformat = DataFormatter(dataDir)

dformat.dict2arff(os.path.join(dataDir, 'System.csv'), os.path.join(dataDir, 'System.arff'))

#Arff_file = os.path.join(dataDir, 'System.arff')

jvm.start(packages=True)

data = converters.load_any_file(os.path.join(dataDir, 'System.arff'))
clusterer = Clusterer(classname="weka.clusterers.SimpleKMeans", options=["-N", "10", "-S", "10"])
clusterer.build_clusterer(data)

# print clusterer
# cluster the data
# for inst in data:
#     cl = clusterer.cluster_instance(inst)  # 0-based cluster index
#     dist = clusterer.distribution_for_instance(inst)   # cluster membership distribution
#     print("cluster=" + str(cl) + ", distribution=" + str(dist))
#     print inst

# serialization.write(os.path.join(modelDir, 'SKM.model'), clusterer)

clusterEM = Clusterer(classname="weka.clusterers.EM", options=["-I", "1000", "-N", "6", "-X", "10", "-max", "-1", "-ll-cv", "1.0E-6", "-ll-iter", "1.0E-6", "-M", "1.0E-6", "-num-slots", "1", "-S", "100"])
clusterEM.build_clusterer(data)
#print clusterEM
# for inst in data:
#     cl2 = clusterEM.cluster_instance(inst)
#     dist2 = clusterEM.distribution_for_instance(inst)
#     print ("cluster=" + str(cl2) + ", distribution=" + str(dist2))
#     print inst
#
clusterDBSCAN = Clusterer(classname="weka.clusterers.DBSCAN", options=["-E",  "0.9",  "-M", "6", "-I", "weka.clusterers.forOPTICSAndDBScan.Databases.SequentialDatabase", "-D", "weka.clusterers.forOPTICSAndDBScan.DataObjects.EuclideanDataObject"])
clusterDBSCAN.build_clusterer(data)

serialization.write(os.path.join(modelDir, "dbscan.model"), clusterDBSCAN)
cluster = Clusterer(jobject=serialization.read(os.path.join(modelDir, "dbscan.model")))
# print clusterDBSCAN
# print clusterDBSCAN.number_of_clusters
for inst in data:
    cl3 = cluster.cluster_instance(inst)
    dist3 = cluster.distribution_for_instance(inst)
    print ("cluster=" + str(cl3) + ", distribution=" + str(dist3))


# for inst in data:
#     cl3 = clusterDBSCAN.cluster_instance(inst)
#     dist3 = clusterDBSCAN.distribution_for_instance(inst)
#     print ("cluster=" + str(cl3) + ", distribution=" + str(dist3))
jvm.stop()
