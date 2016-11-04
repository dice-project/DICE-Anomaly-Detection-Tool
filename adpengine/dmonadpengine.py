import os
from dmonconnector import *
from util import queryParser
settings = {'load': '<model_name>', 'index': 'logstash-*', 'qsize': 'qs', 'from': '1444444444', 'detect': False, 'qinterval': '10s', 'dmonPort': 5001, 'validate': False, 'esendpoint': '85.120.206.27', 'snetwork': 'net_threashold', 'smemory': 'mem_threashold', 'to': '1455555555', 'export': '<model_name>', 'train': True, 'esInstanceEndpoint': 9200, 'MethodSettings': {'set1': 'none', 'set2': 'none', 'set3': 'none'}, 'file': None, 'sload': 'load_threashold', 'query': 'yarn:resourcemanager, clustre, jvm_NM;system', 'model': '<model_name>', 'method': 'method_name'}


class AdpEngine:
    def __init__(self, settingsDict):
        self.esendpoint = settingsDict['esendpoint']
        self.esInstanceEndpoint = settingsDict['esInstanceEndpoint']
        self.dmonPort = settingsDict['dmonPort']
        self.index = settingsDict['index']
        self.tfrom = settingsDict['from']
        self.to = settingsDict['to']
        self.query = settingsDict['query']
        self.qsize = settingsDict['qsize']
        self.qinterval = settingsDict['qinterval']
        self.train = settingsDict['train']
        self.model = settingsDict['model']
        self.load = settingsDict['load']
        self.method = settingsDict['method']
        self.validate = settingsDict['validate']
        self.export = settingsDict['export']
        self.detect = settingsDict['detect']
        self.sload = settingsDict['sload']
        self.smemory = settingsDict['smemory']
        self.snetwork = settingsDict['snetwork']
        self.methodSettings = settingsDict['MethodSettings']
        self.dataDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
        self.modelsDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')
        self.anomalyIndex = "anomalies"
        self.dmonConnector = Connector(self.esendpoint)
        self.qConstructor = QueryConstructor()
        self.dformat = DataFormatter(self.dataDir)

    def initConnector(self):
        print "Establishing connection with dmon ....."
        resdmonInfo = self.dmonConnector.getDmonStatus()
        print "Connection established, status %s" %resdmonInfo
        resInfo = self.dmonConnector.info()
        print "General es dmon info -> %s" %resInfo

        interval = self.dmonConnector.getInterval()

        if int(self.qinterval[:-1]) < interval['System']:
            logger.warning('[%s] : [WARN] System Interval smaller than set interval!',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))
            print "Warning query interval difference detected, dmon interval is %s while adp is %s!" %(self.qinterval, interval['System'])
        else:
            print "Query interval check passed."
            logger.info('[%s] : [INFO] Query interval check passed!',
                           datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'))

        resClusterState = self.dmonConnector.clusterHealth()
        print "ES cluster health -> %s" %resClusterState

        # print "Checking index %s state ...." %self.index
        # resGetIndex = self.dmonConnector.getIndex(self.index)
        # print "Index %s state -> %s" %(self.index, resGetIndex)

        print "Checking dmon registered nodes...."
        nodeList = self.dmonConnector.getNodeList()
        print "Nodes found -> %s" %nodeList

    def getData(self):
        queryd = queryParser(self.query)
        return queryd

    def runmethod(self):
        return "select and run methods"

    def loadModel(self):
        return "model"

    def reportAnomaly(self):
        return "anomaly"

    def printTest(self):
        print "Endpoint -> %s" %self.esendpoint
        print "Method settings -> %s" %self.methodSettings




test = AdpEngine(settings)

test.initConnector()
print test.getData()