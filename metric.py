from threading import Thread
from threading import Semaphore
#from collections import namedtuple as NT

from influxdb import InfluxDBClient
import logging
import time
from datetime import datetime
from datetime import timedelta
import json
# See SNMP.py module at the bottom of the question
from snmp import v2c

from collections import namedtuple as NT
from pysnmp.entity.rfc3413.oneliner import cmdgen
from pysnmp.entity.rfc3413 import mibvar
from pysnmp.proto import rfc1902
from pysnmp.proto import rfc1905
from pysnmp.smi import builder, view, error

import pprint

class Metric(Thread):
    """docstring for Metric"""
    def __init__(self,root,MetricId):
        super(Metric, self).__init__()
        self.metric={}
        self.MetricId=MetricId
        self.address=root.address
        self.community='public'
        self.results = {}
        #root.v2c   #
        self.daemon=True
        self.SnmpMethode='bulkwalk'
        self.dataready=False
        self.root=root
        self.errorIndication=None 
        self.errorStatus=None 
        self.errorIndex=None
        #self.v2c=root.v2c #v2c(self.address) 
        self.kill=False
        self.EnableDataGather=False
        self.cmdGen = cmdgen.CommandGenerator()
        self.SNMPObject = NT('SNMPObject', ['modName', 'datetime', 'symName', 
            'index', 'value'])
        self.query_retries=3
        self.query_timeout=9

        #self.loadFromJson()
        #self.name=self.root.host_data['ID']+'-'+ self.MetricId
        #self.thread=Metric_Thread(self.address)
        
    #def append(self, ID, name, oid, mtype, isTag, isIndexed):
    #    self.id = ID
    #    self.name = name
    #    self.oid = oid
    #    self.type = mtype    # Integer / String ...
    #    self.isTag = isTag   # True / False
    #    self.isIndexed = isIndexed # True / False  # oid.host_id.interface_id = value

    def loadFromJson(self):
        with open('metrics.json') as json_file:
            metricsConfig=json.load(json_file)
        self.metric=metricsConfig[self.MetricId]
        self.name=self.root.host_data['ID']+'-'+ self.root.measurement['ID']+'-'+self.MetricId

    def run(self):

        self.metric=self.root.metrics[self.MetricId]
        self.name=self.root.host_data['ID']+'-'+ self.root.measurement['ID']+'-'+self.MetricId
        if self.metric['isIndexed']:
            self.SnmpMethode='bulkwalk'
        else:
            self.SnmpMethode='get'
        logging.debug("Start  thread %s methode:%s",self.MetricId ,self.SnmpMethode)

        # self.v2c=v2c(self.address) 
        # if self.SnmpMethode == 'bulkwalk' :
        #     ret=self.bulkwalk([self.MetricId])
        #     logging.debug("SNMP bulkwalk %s %s return size:%s ", self.address, self.MetricId, ret)
        # if self.SnmpMethode == 'get' :
        #     ret=self.get([self.MetricId])
        #     logging.debug("SNMP get %s %s return size:%s ",self.address, self.MetricId, ret)
        # del(self.v2c)

        self.EnableDataGather=False                
        while not self.kill:
            if self.EnableDataGather :
                logging.debug("Restart  thread %s",self.MetricId )
                #print(self.metric)
                self.v2c=v2c(self.address) 
                start = time.time()
                if self.SnmpMethode == 'bulkwalk' :
                    ret=self.Mybulkwalk([self.MetricId])
                    logging.debug("SNMP bulkwalk %s %s return size:%s Time: %0.3fsec", self.address, self.MetricId, ret, (time.time()-start) )
                if self.SnmpMethode == 'get' :
                    ret=self.get([self.MetricId])
                    logging.debug("SNMP get %s %s return size:%s Time: %0.3fsec",self.address, self.MetricId, ret , (time.time()-start))
                del(self.v2c)
                self.EnableDataGather=False
                self.kill=True
            time.sleep(1)
        logging.warning("Stop  thread %s",self.MetricId)
        #if self.root.enable :
        
        #print(self.metric)
    def getData(self):
            self.EnableDataGather=True
            logging.debug("EnableDataGather %s",self.MetricId )
    

    def getMetricOid(self,MetricId):
        if isinstance(MetricId, str) :
            result=None
            metric=self.metric[MetricId]
            if metric['Oid']:
                result = metric['Oid']
        if isinstance(MetricId, list) :
            result=list()
            for metric_id in MetricId:
                result.append(self.metric['Oid'])
        #self.MetricId=str(MetricId)
        return result

    def get(self,MetricId):
        self.dataready=False
        MetricOid =self.getMetricOid(MetricId) 
        if MetricOid != None :
            logging.info('Start snmp get %s   %s (%s)' % (self. address, MetricOid, MetricId))
            self.errorIndication, self.errorStatus, self.errorIndex, self.results = self.v2c.get(MetricOid) 
        else:
            logging.warning('Not Start snmp get %s metric not found', MetricId)
        
        if self.results != None:
            self.dataready=True
            return len(self.results)
        else:
            logging.error('SNMP error %s   %s (%s) || %s  %s  %s',self.address, str(MetricOids), str(MetricIds),self.errorIndication, self.errorStatus, self.errorIndex)
            return 0  
    def walk(self,MetricId):
        self.dataready=False
        MetricOid =self.getMetricOid(MetricId) 

        if MetricOid != None :
            logging.info('Start snmp walk %s  Oid: %s  id: %s' % (self. address, MetricOid, MetricId))
        else:
            logging.warning('Not Start snmp walk %s metric not found', MetricId)
        self.results = self.v2c.walk(MetricOid) 
        if self.results != None:
            self.dataready=True
            return len(self.results)
        else:
            return 0   


    def Mybulkwalk(self, MetricIds):
        """SNMP bulkwalk a device.  NOTE: This often is faster, but does not work as well as a simple SNMP walk"""
        #print(oids)
        self.results = None
        retval = list()
        MetricOids =self.getMetricOid(MetricIds) 
        if MetricOids != None :
            logging.info('Start snmp Mybulkwalk %s   %s (%s)' % (self.address, str(MetricOids), str(MetricIds)))
            self.errorIndication, self.errorStatus, self.errorIndex, varBindTable = self.cmdGen.bulkCmd(  
                            cmdgen.CommunityData('test-agent', self.community),  
                            cmdgen.UdpTransportTarget((self.address, 161),  
                            retries=self.query_retries,
                            timeout=self.query_timeout),
                               
                    0,
                    25,
                    #self._format(oid),
                    #','.join(oids)
                    lookupMib=False,
                    *MetricOids
                    )
            if not self.errorIndication:

                for varBindTableRow in varBindTable:
                    for index, val in varBindTableRow:
                        #print("értékv2 ", name, val)
                        #value=self._from_pysnmp(val)
                        value=val.prettyPrint()
                        modName='valami'
                        symName=''
                        retval.append(self.SNMPObject._make([modName,datetime.now(), symName, index.prettyPrint(), value]))
                # for index, val in varBindTable:
                #     #print("érték ", name, val)
                #     #value=self._from_pysnmp(val)
                #     value=val.prettyPrint()
                #     modName=''
                #     symName=''
                #     retval.append(self.SNMPObject._make([modName,datetime.now(), symName, index.prettyPrint(), value]))
                #     logging.debug('Mybulkwalk result %s   %s (%s) %s %s' % (self.address, str(MetricOids), str(MetricIds),index.prettyPrint(), value))
                self.results=retval

    #print(errorIndication, errorStatus, 
        #        errorIndex, varBindTable)
        if self.results != None:
            self.dataready=True
            return len(self.results)
        else:
            logging.warning('SNMP error %s   %s (%s) || %s  %s  %s',self.address, str(MetricOids), str(MetricIds),self.errorIndication, self.errorStatus, self.errorIndex)
            return 0   
    def bulkwalk(self,MetricIds):
        self.dataready=False
        MetricOids =self.getMetricOid(MetricIds) 
        #print("Oids : ")
        #print(MetricOids)
        if MetricOids != None :
            logging.info('Start snmp bulkwalk %s   %s (%s)' % (self.address, str(MetricOids), str(MetricIds)))
            try:
                self.errorIndication, self.errorStatus, self.errorIndex, self.results = self.v2c.bulkwalk(MetricOids) 
            except Exception as s:
                logging.error('Exception snmp bulkwalk %s   %s (%s)' % (self.address, str(MetricOids), str(MetricIds), str(s) ) )
            if self.errorIndication:
                logging.error('Error in snmp bulkwalk %s   %s (%s) %s ' % (self.address, str(MetricOids), str(MetricIds), str(self.errorIndex )) )
        else:
            logging.warning('Not Start snmp bulkwalk %s metric not found', str(MetricIds))
        if self.results != None:
            self.dataready=True
            return len(self.results)
        else:
            logging.warning('SNMP error %s   %s (%s) || %s  %s  %s',self.address, str(MetricOids), str(MetricIds),self.errorIndication, self.errorStatus, self.errorIndex)
            return 0             