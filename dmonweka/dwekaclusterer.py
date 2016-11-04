import os
import tempfile
import traceback
import weka.core.jvm as jvm
import dmonweka.helper as helper
from weka.clusterers import Clusterer
import weka.core.converters as converters
from dataformatter import DataFormatter
import weka.core.packages as packages





dataDir = os.path.join(os.path.dirname(os.path.abspath('')), 'data')



dformat = DataFormatter(dataDir)

dformat.dict2arff(os.path.join(dataDir, 'System.csv'), os.path.join(dataDir, 'System.arff'))

#Arff_file = os.path.join(dataDir, 'System.arff')

jvm.start()

data = converters.load_any_file(os.path.join(dataDir, 'System.arff'))
clusterer = Clusterer(classname="weka.clusterers.SimpleKMeans", options=["-N", "10", "-S", "10"])
clusterer.build_clusterer(data)

print clusterer
# cluster the data
for inst in data:
    cl = clusterer.cluster_instance(inst)  # 0-based cluster index
    dist = clusterer.distribution_for_instance(inst)   # cluster membership distribution
    print("cluster=" + str(cl) + ", distribution=" + str(dist))

clusterEM = Clusterer(classname="weka.clusterers.EM", options=["-I", "1000", "-N", "6", "-X", "10", "-max", "-1", "-ll-cv", "1.0E-6", "-ll-iter", "1.0E-6", "-M", "1.0E-6", "-num-slots", "1", "-S", "100"])
clusterEM.build_clusterer(data)
print clusterEM

clusterDBSCAN = Clusterer(classname="weka.clusterers.DBSCAN", options=["-E",  "0.9",  "-M", "6", "-I", "weka.clusterers.forOPTICSAndDBScan.Databases.SequentialDatabase", "-D", "weka.clusterers.forOPTICSAndDBScan.DataObjects.EuclideanDataObject"])
clusterDBSCAN.build_clusterer(data)
print clusterDBSCAN




jvm.stop()
