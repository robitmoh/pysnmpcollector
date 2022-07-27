from memory_profiler import profile

from threading import Thread, Timer
from threading import Semaphore
from collections import namedtuple as NT

from influxdb import InfluxDBClient
import logging
from logging.handlers import RotatingFileHandler
import time
from datetime import datetime
from datetime import timedelta
import json
import os
import gc
import psutil
# See SNMP.py module at the bottom of the question
#rom snmp import v2c


#rom pysnmp.entity.rfc3413 import mibvar
#rom pysnmp.proto import rfc1902
#rom pysnmp.proto import rfc1905
#rom pysnmp.smi import builder, view, error

from netaddr import *
from binascii import hexlify
from metric_netsnmp import Metric
import netsnmp
from random import randint

import pprint




class NETsnmp(object):
    """docstring for netsnmp"""
    def __init__(self, address='127.0.0.1',community='public'):
        super(NETsnmp, self).__init__()
        self.Version=2
        self.DestHost=address
        self.Community=community
        self.Timeout=15000000
        self.Retries=1
        self.UseNumeric=1
        self.SNMPObject = NT('SNMPObject', ['modName', 'datetime', 'oidtag', 'index', 'value', 'type','secondIndex'])

    def _result(self,value=None):
        result=value
        #print(value.val, str(value.val), hexlify(value.val))
        try:
            if value.type=='OCTETSTR':
                result=str(EUI(hexlify(value.val).decode('UTF-8'), dialect=mac_unix_expanded))
                #result="{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}".format(result)
            if value.type=='INTEGER':
                result=int(value.val)
            if value.type=='INTEGER32':
                result=int(value.val)
        except Exception as s:
            pass
           # self.logging.error('SNMP data convert error', str(s))
        return result

    def snmpwalk(self,Oid):
        IndexResult=list()
        var_list=netsnmp.VarList(Oid)
        netsnmp_session=netsnmp.Session( Version=self.Version, DestHost=self.DestHost, Community=self.Community,  Timeout=self.Timeout, Retries=self.Retries, UseNumeric=self.UseNumeric)
        results = netsnmp_session.walk(var_list)
        modName=self.modName
        if netsnmp_session.ErrorNum == 0 and netsnmp_session.ErrorInd !=-24:
            for result in var_list:
                IndexResult.append(self.SNMPObject._make([modName,datetime.now(), result.tag, result.iid, self._result(result), result.type,'']))
                #IndexResult=[{result.iid: self.SNMPObject._make([modName,datetime.now(), result.tag, result.iid, self._result(result), result.type])}]
        
        return netsnmp_session.ErrorStr,netsnmp_session.ErrorInd,netsnmp_session.ErrorNum, IndexResult


def configure_logger(name, logfile):
    logger = logging.getLogger(name)
    formatter = logging.Formatter('%(asctime)s - %(name)-14s - %(levelname)-8s - %(message)s')
    file_handler = logging.FileHandler(logfile, mode='w')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.setLevel(logging.DEBUG)   
    return logger     

class measurement_Thread(Thread):
    def __init__(self, root, host,MeasurementID):
        Thread.__init__(self)
        self.host_data=host
        self.root=root
        self.address = self.host_data['Host']
        self.community='public'
        #self.measurementsConfig=root.measurementsConfig #list()
        self.MeasurementID=MeasurementID
        self.measurement=root.measurementsConfig[self.MeasurementID]
        #self.measurement={}
        self.metrics=root.metrics #{}
        self.config=root.config
        self.managers=set()
        self.finished=set()
        self.IndexResult=list()
        self.SecondIndexResult=list()
        self.SNMPObject = NT('SNMPObject', ['modName', 'datetime', 'oidtag', 'index', 'value', 'type','secondIndex'])
        self.IndexTime=None
        self.kill=False
        #self.influx=None
        self.influx=self.root.influx
        self.enable=False
        self.EnableDataGather=False
        #self.host_data=host
        self.Index_ErrorIndication=False
        self.MeasurementReady=False
        self.name=self.host_data['ID']+'-'+ self.measurement['ID']
        self.updateFreq=self.measurement['updateFreq']
        self.indexUpdateFreq=self.measurement['IndexCache']
        #self.netsnmp=netsnmp
        #self.netsnmp_session=netsnmp.Session( Version=2, DestHost=self.address, Community=self.community,  Timeout=1000000, Retries=3, UseNumeric=1)
        self.timeoutTime=240
        self.timeout=False
        self.waitMeasurementData=1
        self.processSleep=1  # run function sleep time
        self.isDoubleIndexed=False
        for MetricId in self.measurement['MetricIDs']:
            try:
                isDoubleIndexed=self.metrics[MetricId]['isDoubleIndexed']
            except:
                isDoubleIndexed=False
            if isDoubleIndexed:            
                self.isDoubleIndexed=True
        self.indexready=False
        self.SecondIndexTime=None
        self.SecondIndex_ErrorIndication=False
        self.SecondIndexReady=False
        self.SecondIndexResult=list()
        self.dumpInfluxData=self.config['influx']['dump_influx']
        self.isRunning=False
        self.processTime=0
        self.MeasurementMetricError=0

        self.logfile=self.config['logging']['logdir']+'/host_logs/'+self.host_data['ID']+'-'+self.MeasurementID+'.log'
        #self.logfile=''
        
        self.logging = logging.getLogger(self.host_data['ID']+'-'+self.MeasurementID)
        self.logging_handler=logging.FileHandler(self.logfile, mode='w')
        self.log_rotating_handler = RotatingFileHandler(self.logfile, maxBytes=int(self.config['logging']['measurement_maxBytes']), backupCount=int(self.config['logging']['measurement_backupCount']))
        self.logging_formatter = logging.Formatter('%(asctime)s - %(name)-14s - %(levelname)-8s - %(message)s')
        self.logging_handler.setFormatter(self.logging_formatter)
        self.logging.addHandler(self.log_rotating_handler)
        self.logging.addHandler(self.logging_handler)
        self.logging.setLevel(self.config['logging']['measurement_level'].upper())   
        #(, self.logfile) #logging.getLogger(self.MeasurementID)
        try :
            self.logging.disabled= self.measurement['log']
        except:
            self.logging.disabled = False

        try :
            self.dumpIndex= self.measurement['dumpIndex']
        except:
            self.dumpIndex = False
        
        self.logging.propagate= False

        #self.netsnmp_session=netsnmp.Session( Version=2, DestHost=self.address, Community=self.community,  Timeout=1500000, Retries=3, UseNumeric=1)
        #self.netsnmp=netsnmp
        
        #self.isDoubleIndexed=True
    
    

    def indexed_result(self,data):
        indexed_result={}
        try:
            for row in data:
                    indexed_result[int(row.index)]=row.value
            return indexed_result
        except:
            self.logging.error('Walk data indexing error %s   ',self.MeasurementID)


    def getIndex(self):
        Oid=self.measurement['IndexOid']
        self.Index_ErrorIndication=False
        now=datetime.now()
        start=time.time()
        if not self.IndexTime  or now - self.IndexTime > timedelta(seconds=int(self.measurement['IndexCache'])):
            self.logging.info("Get measurement index data for : %s IndexOid: %s",self.MeasurementID, Oid)
            
            try:
                snmp=NETsnmp(self.address,self.community)
                snmp.modName=self.MeasurementID
                ErrorStr,ErrorInd,ErrorNum, IndexResult=snmp.snmpwalk(Oid)            
                del snmp
            except Exception as s:
                self.logging.debug('index query error %s   ',s)

            if ErrorNum == 0 and ErrorInd !=-24:
                self.indexready=True
                self.IndexTime=datetime.now()
                #self.processSleep=1
                self.logging.info("Index data for : %s IndexOid: %s  Time  %0.3fsec ",self.MeasurementID, Oid ,(time.time()-start) )                
                self.IndexedIndexResult=self.indexed_result(IndexResult)
                if self.dumpIndex:
                    file=self.config['base']['tmp_dir']+'/json_dump/{}-{}-Index.json'.format( self.host_data['ID'], self.MeasurementID )
                    self.root.dumpjson(file,self.IndexedIndexResult)

                return
            else:
                self.Index_ErrorIndication=True
                self.indexready=False
                self.IndexTime=datetime.now()
                #self.processSleep=self.measurement['IndexCache']
                self.logging.error('SNMP IndexQuery error ErrorStr:%s  ErrorInd:%s ErrorNum:%s ', 
                                ErrorStr,ErrorInd,ErrorNum)
                #self.logging.error('Next Index query after %s sec.  Stop the metrics ', self.measurement['IndexCache'] )
                self.timeout=True
                #self.stopMetrics()
                return None
        else:
            if not self.Index_ErrorIndication :
                self.logging.debug("cache valid: %s",  self.MeasurementID)
                return
    
    # def getIndexes(self):
    #     self.IndexResult=self.getIndex()


    def getSecondIndex(self):
        try:
            Oid=self.measurement['SecondIndexOid']
            #SecondIndexCache=self.measurement['SecondIndexCache']
            SecondIndexTag=self.measurement['SecondIndexTag']
        except Exception as s:
            self.logging.error("Second index error please specify the SecondIndexOid, SecondIndexTag element in measurments.json : %s %s",self.MeasurementID,s)
        self.SecondIndex_ErrorIndication=False
        now=datetime.now()
        start=time.time()
        if not self.SecondIndexTime  or now - self.SecondIndexTime > timedelta(seconds=int(self.measurement['IndexCache'])):
            self.logging.info("Get Hostindex data for : %s IndexOid: %s",self.MeasurementID,Oid)
            
            #ErrorStr,ErrorInd,ErrorNum, IndexResult=self.snmpwalk(Oid)     
            snmp=NETsnmp(self.address,self.community)
            snmp.modName=self.MeasurementID
            ErrorStr,ErrorInd,ErrorNum, IndexResult=snmp.snmpwalk(Oid)                   
            del snmp
            if ErrorNum == 0 and ErrorInd !=-24:
                self.SecondIndexReady=True
                self.SecondIndexTime=datetime.now()
                #self.processSleep=1
                self.logging.info("SecondIndex data for : %s IndexOid: %s  Time  %0.3fsec ",self.MeasurementID, Oid, (time.time()-start) )                
                self.SecondIndexedIndexResult=self.indexed_result(IndexResult)
                if self.dumpIndex:
                    file=self.config['base']['tmp_dir']+'/json_dump/{}-{}-SecondIndex.json'.format( self.host_data['ID'], self.MeasurementID )
                    self.root.dumpjson(file,self.SecondIndexedIndexResult)

                return 
            else:
                self.SecondIndex_ErrorIndication=True
                self.SecondIndexReady=False
                self.SecondIndexTime=datetime.now()
                #self.processSleep=self.measurement['IndexCache']
                self.logging.error('SNMP SecondIndexQuery error ErrorStr:%s  ErrorInd:%s ErrorNum:%s ', 
                                ErrorStr,ErrorInd,ErrorNum)
                #self.logging.error('Next SecondIndex query after %s sec.  Stop the metrics ', self.measurement['IndexCache'] )
                self.timeout=True
                #self.stopMetrics()
                return None
        else:
            if not self.SecondIndex_ErrorIndication :
                    self.logging.debug("SecondIndex cache valid: %s",  self.MeasurementID)
                    return 

   

    def getIndexes(self):
        self.getIndex()
        if self.isDoubleIndexed:
            self.getSecondIndex()
        self.logging.debug("IndexGather IndexReady:%s IndexError:%s SecondIndexReady:%s SecondIndexError:%s", self.indexready, self.Index_ErrorIndication,self.SecondIndexReady, self.SecondIndex_ErrorIndication )
        self._indextimer = Timer(interval=self.indexUpdateFreq, function=self.getIndexes)
        self._indextimer.start()

        # IndexResult=self.getIndex()
        # if not self.Index_ErrorIndication :
        #     self.IndexedIndexResult=self.indexed_result(IndexResult)
        # if self.isDoubleIndexed:
        #     SecondIndexResult=self.getSecondIndex()
        #     if not self.SecondIndex_ErrorIndication :
        #         self.SecondIndexedIndexResult=self.indexed_result(SecondIndexResult)

       
  

            
    def startMetrics(self):
        self.logging.warning("Start Measurement : %s :: %s ",self.host_data['ID'], self.MeasurementID)
        self.managers=set()
        if not self.indexready:
            return
        if self.isDoubleIndexed:
            if not self.SecondIndexReady:
                return

        for MetricId in self.measurement['MetricIDs']:
                    self.logging.info("Start Metric thread measurement: %s :: %s Metric: %s",self.host_data['ID'], self.MeasurementID, MetricId)
                    metric=Metric(self,MetricId) 
                    metric.start()
                    self.managers.add(metric)

    def stopMetrics(self):
        if len(self.managers):
            self.logging.warning("Stop Measurement metrics: %s :: %s ",self.host_data['ID'], self.MeasurementID)
            for metric_instance in self.managers:
                metric_instance.kill=True
            #self.managers=set()
            del(self.managers)
            gc.collect()

    def runMetrics(self):
        #if len(self.managers)==0:
        #    self.startMetrics()
        self.logging.warning("Run Measurement : %s :: %s Runnable Metric number:: %s",self.host_data['ID'], self.MeasurementID, len(self.managers))
        for metric_instance in self.managers:
            metric_instance.getData()
        self.EnableDataGather=False
    
    def setEnable(self):
        self.enable=True

    def SetEnableDataGather(self):
        self.EnableDataGather=True
        self.logging.warning("Enable DataGather")

    def waitingMeasurementData(self):
        self.logging.warning("waitingMeasurementData")
        self.MeasurementMetricError=0
        while not self.MeasurementReady :
            if self.kill:
                break
            n=0
            for metric_instance in self.managers:
                metric_instance.join(0.5)
                if metric_instance.MetricError ==True :
                    self.MeasurementMetricError +=1 
                if metric_instance.dataready ==True or metric_instance.MetricError ==True :
                    n=n+1
                else:
                    pass
                    #self.logging.debug("Measurement metric: %s data gather is NOT ready: %s subprocess(%s/%s)", metric_instance.MetricId, metric_instance.dataready,len(self.managers),n)

            if n >= len(self.managers):
                self.MeasurementReady=True
                break
            if int(time.time()-self.start) >self.timeoutTime:
                self.timeout=True
                break

            time.sleep(self.waitMeasurementData)
    def DataGather(self):
        self.logging.error("Starting DataGather")

        proc = psutil.Process()                    
        if proc.num_fds() >int(self.config['base']['fd_limit_low']):
            time.sleep(0.1)
        if proc.num_fds() >int(self.config['base']['fd_limit_high']):
            wait=time.time() 
            while proc.num_fds() >int(self.config['base']['fd_limit_low'])-100:
                time.sleep(0.1)
            print("Többet Várunk",time.time()-wait, proc.num_fds())

        self.isRunning=True
        self.start = time.time()
        self.timeout=False
        self.startTime=datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        
        #try:
        #    self.getIndexes()
        #except Exception as s:
        #    self.logging.error("Error in index process: %s", s )
        if self.Index_ErrorIndication==False and self.SecondIndex_ErrorIndication==False:
            self.MeasurementReady=False
            try:
                self.startMetrics()
                self.runMetrics()
                self.waitingMeasurementData() 
            except Exception as s:
                self.logging.error("Error in data process: %s", s )
        else:
            self.logging.error("No index data")

        self.processTime=time.time()-self.start
        if not self.Index_ErrorIndication and self.MeasurementReady:
            self.logging.error("Measurement data ready %s %s Time: %0.3f sec MetricError: %s",self.host_data['ID'], self.MeasurementID, self.processTime, self.MeasurementMetricError)
            self.getJsonResult()
            #self.root.statistic.AddMeasurmentTime(self.processTime)
        if self.timeout:
            self.logging.error("Measurement timeout %s %s Time: %0.3f sec",self.host_data['ID'], self.MeasurementID, self.processTime)
            #self.root.statistic.IncTimeoutedMeasurment()
        self.stopMetrics()
        self.isRunning=False
        self._timer = Timer(interval=self.updateFreq, function=self.DataGather)
        self._timer.start()



    def Setkill(self):
            try:
                self.stopMetrics()
            except:
                pass
            self.kill=True
        
   
    def run(self):
        #while True:
        self._indextimer = Timer(interval=randint(1,2), function=self.getIndexes)
        self._indextimer.start()
        self._timer = Timer(interval=randint(10,self.updateFreq/2), function=self.DataGather)
        self._timer.start()

        while not self.kill:
            try:
                if self.enable:
                    #self.root.statistic.IncActiveMeasurment()
                    #self.root.statistic.IncRunningMeasurment()
                    #if self.EnableDataGather:
                    #    self.DataGather()                        
                    pass
                #self.root.statistic.DecRunningMeasurment()
                #time.sleep(int(self.processSleep))
            except Exception as s:
                self.logging.error("Error in running process: %s", s )
            time.sleep(10)
    

    # def getResult(self):
        
    #     #while (len(self.finished)<len(self.managers)):
    #     self.logging.info("Runing measurement threads: %s", len(self.managers))
    #     for instance in self.managers.difference(self.finished):
    #         instance.join(0.5)
    #         #print(instance.results)
    #         if not instance.is_alive():
    #             self.finished.add(instance)
    #             print ("RESULT:", instance.results)
    #             for result in instance.results:
    #                 #print ( "%s=%s"  % (result.index, result.value ))
    #                 print(result.index, result.value)
    #     #print ("   Execution time: %0.3f seconds" % ((time.time()-start)))


    

    def writeInflux(self,MeasurementDatas):
        if len(MeasurementDatas):
                if self.dumpInfluxData:
                    file=self.config['base']['tmp_dir']+'/json_dump/{}-{}.json'.format( self.host_data['ID'], self.MeasurementID )
                    self.root.dumpjson(file,MeasurementDatas)
                    
                if len(MeasurementDatas) <10:
                    #pprint.pprint(MeasurementDatas)
                    self.logging.debug('influx data:  %s', MeasurementDatas )
                self.logging.error("Write influx data %s for host=%s measurement=%s record:%s",  self.startTime, self.host_data['ID'], self.MeasurementID ,len(MeasurementDatas) )
                try:
                    self.influx.write(MeasurementDatas)
                except Exception as s:
                    self.logging.error('influx error'+ str(s))

                self.MeasurementReady=False
                for metric_instance in self.managers:
                    metric_instance.results={}
                    metric_instance.dataready =False
        else:
            self.logging.error('Error: No influx Data')

                                        


# {
#   'fields': {'docsIf3CmtsCmUsStatusCorrecteds': 0,
#              'docsIf3CmtsCmUsStatusSignalNoise': 330,
#              'docsIf3CmtsCmUsStatusUncorrectables': 178176,
#              'docsIf3CmtsCmUsStatusUnerroreds': 5},
#   'measurement': 'cmts',
#   'tags': {'Index': '235',
#            'SecondIndex': 721457,
#            'cm': '24:76:7d:4f:4a:24',
#            'cmts': 'FivanC4',
#            'docsIfUpChannelFrequency': '61800000'},
#   'time': '2020-07-31T08:55:50Z'
# }


    def getJsonResult_v5(self):
        meas_data=[]
        res={}
        self.logging.debug("Get JsonResoult Mode:%s host:%s Measurment:%s MeasurementReady:%s", self.measurement['Mode'], self.address, self.MeasurementID,self.MeasurementReady)
        if self.MeasurementReady:
            try: 
                for indexKey in self.IndexedIndexResult: 
                    indexValue=self.IndexedIndexResult[indexKey]
                    res.update({indexKey:{}})
                    tmp_data={}
                    for metric_instance in self.managers:
                        if metric_instance.isDoubleIndexed:
                            for secIndexKey in self.SecondIndexedIndexResult:

                                secIndexValue=self.SecondIndexedIndexResult[secIndexKey]
                                #res[indexKey]={secIndexKey:{}}
                                indexed_value=None
                                tags={}
                                fields={}
                                if metric_instance.metric['isTag']:
                                    try:
                                        indexed_value=metric_instance.indexed_result[indexKey][secIndexKey]
                                        tags={metric_instance.MetricId:indexed_value}
                                    except Exception as s:
                                        pass
                                        #self.logging.error('Tag append error %s', s)
                                else:
                                    try:
                                        indexed_value=metric_instance.indexed_result[indexKey][secIndexKey]
                                        fields={metric_instance.MetricId:indexed_value}
                                    except Exception as s:
                                        pass
                                        #self.logging.error('fields append error %s', s)
                                if len(tags)>0 or len(fields)>0:
                                    try:
                                        test=res[indexKey][secIndexKey]

                                    except:
                                        res[indexKey].update({secIndexKey:{"measurement": self.measurement['ID'],"time": self.startTime,'tags':{}, 'fields':{}}})
                                        res[indexKey][secIndexKey]['tags'].update({self.measurement['IndexTag']:indexValue.upper()})
                                        res[indexKey][secIndexKey]['tags'].update({self.measurement['SecondIndexTag']:secIndexValue})
                                        res[indexKey][secIndexKey]['tags'].update({'indexKey':indexKey})
                                        res[indexKey][secIndexKey]['tags'].update({'secIndexKey':secIndexKey})

                                    res[indexKey][secIndexKey]['tags'].update(tags.copy())
                                    res[indexKey][secIndexKey]['fields'].update(fields.copy())
                        else:
                            indexed_value=None
                            tags={}
                            fields={}
                            if metric_instance.metric['isTag']:
                                try:
                                    indexed_value=metric_instance.indexed_result[indexKey]
                                    tags={metric_instance.MetricId:indexed_value}
                                except Exception as s:
                                    pass
                                    #self.logging.error('Tag append error %s', s)
                            else:
                                try:
                                    indexed_value=metric_instance.indexed_result[indexKey]
                                    fields={metric_instance.MetricId:indexed_value}
                                except Exception as s:
                                    pass
                                    #self.logging.error('fields append error %s', s)
                            #if len(tags)>0 or len(fields)>0:
                            if len(tags)>0 or len(fields)>0:
                                try:
                                    test=res[indexKey][0]
                                except:
                                    res[indexKey].update({0:{"measurement": self.measurement['ID'],"time": self.startTime,'tags':{}, 'fields':{}}})
                                    res[indexKey][0]['tags'].update({self.host_data['DeviceTagName']:self.host_data['ID']})
                                    res[indexKey][0]['tags'].update({self.measurement['IndexTag']:indexValue})
                                    res[indexKey][0]['tags'].update({'indexKey':indexKey})

                                res[indexKey][0]['tags'].update(tags.copy())
                                res[indexKey][0]['fields'].update(fields.copy())
                           # print(indexKey, res)
                                    #self.logging.debug('res data:  %s', res )
                    #if indexKey >20:
                    #    break

                    
                for index in res:
                    for secindex in res[index]:
                        #print(res[index][secindex])
                        if len(res[index][secindex]['fields']) >0:
                            meas_data.append(res[index][secindex])
            except Exception as s:
                self.logging.error("Json data error %s %s :: %s", self.host_data['ID'],  self.measurement['ID'] ,s)
        self.writeInflux(meas_data)



    

    def getJsonResult(self):
        self.getJsonResult_v5()
        #self._timer = Timer(self.updateFreq, self.DataGather())
        #self._timer.start()
     


