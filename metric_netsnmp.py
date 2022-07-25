from threading import Thread
from threading import Semaphore
from collections import namedtuple as NT


import logging
import time
from datetime import datetime
from datetime import timedelta
import json
import psutil
# See SNMP.py module at the bottom of the question



import netsnmp
from binascii import hexlify
from netaddr import *

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
        #self.v2c=root.v2c #v2c(self.address) 
        self.EnableDataGather=False
        #self.cmdGen = cmdgen.CommandGenerator()
        self.SNMPObject = NT('SNMPObject', ['modName', 'datetime', 'oidtag', 'index', 'value', 'type', 'secondIndex'])
        self.SnmpQueryRetries=3
        self.SnmpQueryTimeout=1500000
        self.netsnmp_session=netsnmp.Session( Version=2, DestHost=self.address, Community=self.community,  Timeout=self.SnmpQueryTimeout, Retries=self.SnmpQueryRetries, UseNumeric=1)
        self.netsnmp=netsnmp
        self.kill=False
        self.datalogging=False
        self.MetricError=False
        
        self.isTag=False
        self.isIndexed=False
        self.isDoubleIndexed=False       
        self.logging=root.logging
        try:
            self.metric=self.root.metrics[self.MetricId]
        except:
            self.logging.error("Not defined Metric %s ",self.MetricId)
            return
 
        try:
            if self.metric['isTag']:
                self.isTag=True
            else:
                self.isTag=False
            if self.metric['isIndexed']:
                self.isIndexed=True
            else:
                self.isIndexed=False    
        except Exception as e:
            raise e
        
        try:
            if self.metric['isDoubleIndexed']:            
                self.isDoubleIndexed=True
            else:
                self.isDoubleIndexed=False
        except:
            self.isDoubleIndexed=False
      
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

    def Setkill(self):
            self.kill=True

    def run(self):
        self.name=self.root.host_data['ID']+'-'+ self.root.measurement['ID']+'-'+self.MetricId
  
        if self.isIndexed:
            self.SnmpMethode='walk'
        else:
            self.SnmpMethode='get'
        self.logging.debug("Start thread %s methode:%s",self.MetricId ,self.SnmpMethode)

        self.EnableDataGather=False                
        while not self.kill:
            if self.EnableDataGather :
                self.logging.debug("Start data gathering %s",self.MetricId )
                #print(self.metric)
                #self.v2c=v2c(self.address) 
                start = time.time()
                if self.SnmpMethode == 'walk' :
                    ret=self.walk(self.MetricId)
                    self.logging.debug("SNMP walk %s %s return size:%s Time: %0.3fsec", self.address, self.MetricId, ret, (time.time()-start) )
                if self.SnmpMethode == 'get' :
                    ret=self.get(self.MetricId)
                    self.logging.debug("SNMP get %s %s return size:%s Time: %0.3fsec",self.address, self.MetricId, ret , (time.time()-start))
                #del(self.v2c)
                self.EnableDataGather=False
                #self.kill=True
            time.sleep(1)
        self.logging.warning("Stop thread %s",self.MetricId)
        #if self.root.enable :
        
        #print(self.metric)
    def getData(self):
            self.EnableDataGather=True
            self.logging.debug("EnableDataGather %s",self.MetricId )
    

    def getMetricOid(self,MetricId):
        if isinstance(MetricId, str) :
            result=None
            #metric=self.metric[MetricId]
            if self.metric['Oid']:
                result = self.metric['Oid']
        if isinstance(MetricId, list) :
            result=list()
            for metric_id in MetricId:
                result.append(self.metric['Oid'])
        #self.MetricId=str(MetricId)
        return result

    def _result(self,value=None):
        try:
            result=value.val
            if value.type=='OCTETSTR':
                result=EUI(hexlify(value.val).decode('UTF-8'), dialect=mac_unix_expanded)
            if value.type=='INTEGER':
                result=int(value.val)
            if value.type=='INTEGER32':
                result=int(value.val.decode('UTF-8'))
            if value.type=='IPADDR':
                result=value.val
            if value.type=='COUNTER':
                result=int(value.val)
        except Exception as s:
            self.logging.error('SNMP data convert error  value:', str(s), str(value.val))
        return result

    def get(self,MetricId):
        self.dataready=False
        retval=list()
        MetricOid =self.getMetricOid(MetricId) 
        if MetricOid != None :
            self.logging.info('Start snmpget %s   %s (%s)' % (self.address, MetricOid, MetricId))
            #self.netsnmp_session.ErrorNum, self.netsnmp_session.ErrorStr , self.netsnmp_session.ErrorInd, self.results = self.v2c.get(MetricOid) 
            #get = self.netsnmp.snmpget(var_list, Version=2, DestHost=self.address, Community=self.community)
            self.netsnmp_session.verbose=1 
            var_list=self.netsnmp.VarList(self.netsnmp.Varbind(MetricOid))
            get =self.netsnmp_session.get(var_list)
            # var_list.type
            modName=MetricId
            #retval.append(self.SNMPObject._make([modName,datetime.now(), symName, var_list.iid, hexlify(var_list.val)]))
           
            
            
            for result in var_list:
                retval.append(self.SNMPObject._make([modName,datetime.now(), result.tag, result.iid, self._result(result), result.type,None]))

                if self.datalogging:
                     self.logging.debug('SNMP DATA: %s .%s:: %s [%s]',  result.tag, result.iid, self._result(result), result.type )

            self.logging.debug('SNMP %s result ErrorStr:%s  ErrorInd:%s ErrorNum:%s ResultNum:%s', 
                self.SnmpMethode,self.netsnmp_session.ErrorStr,self.netsnmp_session.ErrorInd,self.netsnmp_session.ErrorNum, len(retval) )

        else:
            self.logging.warning('Not Start snmp get %s metric not found', MetricId)
        
        self.results=retval
        #self.logging.warning('SnmpGet %s result', self.results)
        if self.netsnmp_session.ErrorNum == 0 and self.netsnmp_session.ErrorInd !=-24:
            self.dataready=True
            return len(self.results)
        else:
            self.MetricError =True    
            self.logging.error('SNMP error %s   %s (%s) || %s  %s  %s',self.address, str(MetricOid), str(MetricId),self.netsnmp_session.ErrorNum, self.netsnmp_session.ErrorStr , self.netsnmp_session.ErrorInd)
            return 0  
    def walk(self,MetricId):
        self.dataready=False
        retval=list()
        MetricOid =self.getMetricOid(MetricId) 

        if MetricOid != None :
            self.logging.info('Start snmp walk %s  Oid: %s  id: %s' % (self.address, MetricOid, MetricId))
            var_list=self.netsnmp.VarList(self.netsnmp.Varbind(MetricOid))
            results = self.netsnmp_session.walk(var_list)  #getbulk(0,5000,var_list)
            modName=MetricId
            secondIndex=None
            if self.netsnmp_session.ErrorNum == 0 and self.netsnmp_session.ErrorInd !=-24:
                for result in var_list:
                    if self.isDoubleIndexed:
                        secondIndex=result.iid # double index id
                        try:
                            index=int(result.tag.split('.')[-1:][0])
                        except Exception as s:
                            self.logging.error("Oid split error %s %s %s",s, result.tag , self.netsnmp_session.ErrorStr)
                    else:
                        index=result.iid
                    if result.tag != None and result != None:
                        retval.append(self.SNMPObject._make([modName,datetime.now(), result.tag, index, self._result(result), result.type , secondIndex]))
                        if self.datalogging:
                            self.logging.debug('SNMP DATA: %s ID1:%s ID2:%s :: %s [%s]',  result.tag,index, secondIndex, self._result(result), result.type )
                    else:
                        self.logging.debug('SNMP DATA Error: %s ',  result)
            else:
                self.MetricError =True    
                self.logging.debug('SNMP %s result ErrorStr:%s  ErrorInd:%s ErrorNum:%s ResultNum:%s',self.SnmpMethode,self.netsnmp_session.ErrorStr,self.netsnmp_session.ErrorInd,self.netsnmp_session.ErrorNum, len(retval) )
        else:
            self.logging.warning('Not Start snmp walk %s metric not found', MetricId)
       
        #self.results=retval

        self.SetIndexedResult(retval)
        
        if self.netsnmp_session.ErrorNum == 0 and self.netsnmp_session.ErrorInd !=-24:
            self.dataready=True
            return len(self.results)
        else:
            self.logging.error('SNMP error %s   %s (%s) || %s  %s  %s',self.address, str(MetricOid), str(MetricId),self.netsnmp_session.ErrorNum, self.netsnmp_session.ErrorStr , self.netsnmp_session.ErrorInd)
            return 0   

    # Set the indexed Dict 
    def SetIndexedResult(self,data):
        self.indexed_result={}
        preIndex=None
        precSecondIndex=None
        try:
            secondIndexes={}
            for row in data:
                if row.secondIndex==None :
                    self.indexed_result[int(row.index)]=row.value
                else:
                    if preIndex == row.index or preIndex == None:
                        secondIndexes[int(row.secondIndex)]=row.value
                    else:
                        self.indexed_result[int(preIndex)]=secondIndexes
                        secondIndexes={}
                        secondIndexes[int(row.secondIndex)]=row.value
                preIndex=row.index
                precSecondIndex=row.secondIndex
           # print(self.indexed_result)

        except Exception as s:
            self.logging.error('Walk data indexing error %s  %s ',self.MetricId, s)
    