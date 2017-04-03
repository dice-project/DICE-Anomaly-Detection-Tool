from dataformatter import DataFormatter
from pyQueryConstructor import QueryConstructor
import os
from dmonconnector import Connector

if __name__ == '__main__':
    # dataDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
    dataDir = '/Users/Gabriel/Documents/workspaces/diceWorkspace/dmon-adp/data'
    modelDir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'models')

    #Standard query values
    # qte = 1475842980000
    # qlte = 1475845200000
    qgte = 1481583300000
    qlte = 1481583600000

    qsize = 0
    qinterval = "10s"
    dmonEndpoint = '85.120.206.27'
    index = "lscassandra"
    host = "dice.cassandra.one"

    dmonConnector = Connector(dmonEndpoint, index=index)
    qConstructor = QueryConstructor()
    dformat = DataFormatter(dataDir)

    mongodbCounter, mongodbCounter_file = qConstructor.mongodbCounterString(host=host)
    print "Query string -> %s" % mongodbCounter
    qmongodbCounter = qConstructor.mongoDBCounterQuery(mongodbCounter, qgte, qlte, qsize, qinterval)
    print "Query -> %s" % qmongodbCounter
    gmongodbCounter = dmonConnector.aggQuery(qmongodbCounter)

    print "Response:"
    print gmongodbCounter
    dformat.dict2csv(gmongodbCounter, qmongodbCounter, mongodbCounter_file)

    mongodbGauge, mongodbGauge_file = qConstructor.mongodbGaugeString(host=host)
    print "Query string -> %s" % mongodbGauge
    qmongodbGauge = qConstructor.mongoDBGaugeQuery(mongodbGauge, qgte, qlte, qsize, qinterval)
    print "Query -> %s" % qmongodbGauge
    gmongodbGauge = dmonConnector.aggQuery(qmongodbGauge)

    print "Response:"
    print gmongodbGauge
    dformat.dict2csv(gmongodbGauge, qmongodbGauge, mongodbGauge_file)