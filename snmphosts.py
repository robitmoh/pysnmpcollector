from memory_profiler import profile
from threading import Thread
from threading import Semaphore

import logging
import time
import gc
import sys
from pympler import asizeof

from datetime import datetime
from datetime import timedelta
from random import randint
import json
import psutil
#from pysnmp.entity.rfc3413 import mibvar
#from pysnmp.proto import rfc1902
#from pysnmp.proto import rfc1905
#from pysnmp.smi import builder, view, error

from influxdb import InfluxDBClient
from measurement import measurement_Thread 

class influxdb(object):
        """docstring for influxdb"""
        def __init__(self, config):
            super(influxdb, self).__init__()
            self.config = config
            self.client = InfluxDBClient(host=self.config['influx']['server'], port=self.config['influx']['port'], username=self.config['influx']['user'], password=self.config['influx']['password'], ssl=self.config['influx']['ssl'], verify_ssl=self.config['influx']['verify_ssl'])
            #self.client.create_database('pytest')                
            self.client.switch_database(self.config['influx']['database'])
        
        def write(self,json_body):
            pass
            self.client.write_points(json_body ,retention_policy=self.config['influx']['policy'])

class ObjEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj,measurement_Thread):
            return None
        if isinstance(obj,bytes):
            return str(obj)
        return json.JSONEncoder.default(self, obj)    

class SnmpHosts(Thread):
    @profile
    def __init__(self , event, config, statistic, hosts):
        Thread.__init__(self)
        #self.hosts=list()
        self.event=event
        self.RunningMeasurement=set()
        self.influx=influxdb(config)
        self.config=config
        self.statistic=statistic
        self.hosts=hosts
        self.startHost=0
        self.lastHost=len(self.hosts)
        self.loadFromJson()
        self.hostData={}
        
    def loadFromJson(self):
        logging.info('Load Measurments Metrics')
        #with open('server.json') as json_file:
        #    self.hosts=json.load(json_file)
        #self.lastHost=len(self.hosts)

        with open('./cfg/measurements.json') as json_file:
            self.measurementsConfig=json.load(json_file)
       
        
        with open('./cfg/metrics.json') as json_file:
            self.metrics=json.load(json_file)

   #  def run(self):
   #      logging.warning("Add host measurements: %s %s:%s" ,len(self.hosts) ,self.startHost,self.lastHost)
   #      #print(self.hosts)
   #      try:
   #          for  host in self.hosts[int(self.startHost):int(self.lastHost)]:
   #          	if host['Enable']==True:
	  #               for MeasurementID in host['MeasurementsIDs']:

	  #                   # if self.measurementsConfig[MeasurementID]['Mode'] =='doubleindextag':
	  #                   #     logging.warning("Add doubletag measurements")
	                        
	  #                   # else: 
	  #                   # 	measurement_instance=measurement_Thread(self,host,MeasurementID)
	  #                   measurement_instance=measurement_Thread(self,host,MeasurementID)
	  #                   measurement_instance.setDaemon(True)
	  #                   measurement_instance.start()
	  #                   logging.debug("Add instance to pool %s  %s",MeasurementID,measurement_instance )
	  #                   self.RunningMeasurement.add(measurement_instance)
   #      except Exception as s:
   #          logging.info("Measurment hosts error %s  (%s : %s ) ", s,self.startHost,self.lastHost )
   #          return    
   #      logging.warning("Start %s instances ", len(self.RunningMeasurement))
   #      for host_instance in self.RunningMeasurement:
   #          logging.warning("Enable host measurement thread %s -- %s",host_instance.host_data['ID'], host_instance.MeasurementID)
   #          host_instance.setEnable()

   #      while True:
   #          logging.warning("Runing hosts %s", len(self.RunningMeasurement))
   #          for host_instance in self.RunningMeasurement:
   #              logging.debug("Start Data gather Instance %s is_alive: %s state:%s", host_instance, host_instance.is_alive(),host_instance.enable)
   #              host_instance.EnableDataGather=True
   #              if host_instance.is_alive():
   #                  #host_instance.join(0.5)
   #                  if host_instance.enable:
   #                      logging.debug("Runing Metric in host:'%s' measurement: %s thread num: %s",host_instance.host_data['ID'], host_instance.MeasurementID, len(host_instance.managers))
   #          time.sleep(300)

            
   # 
    #@profile
    def startMeasurments(self,host):
        try:
            test=self.hostData[host['ID']]
        except:
            self.hostData.update({host['ID']:{}})   
    
        for MeasurementID in host['MeasurementsIDs']:
           
            try:
                test=self.hostData[host['ID'][MeasurementID]]
            except:
                self.hostData[host['ID']].update({ MeasurementID:{} })

            self.hostData[host['ID']][MeasurementID].update({'start':time.time()})
            
            # if self.measurementsConfig[MeasurementID]['Mode'] =='doubleindextag':
            #     logging.warning("Add doubletag measurements")
                
            # else: 
            #   measurement_instance=measurement_Thread(self,host,MeasurementID)
            measurement_instance=measurement_Thread(self,host,MeasurementID)
            measurement_instance.setDaemon(True)
            #measurement_instance.EnableDataGather=True
            #measurement_instance.setEnable()
            measurement_instance.start()
            logging.debug("Add instance to pool %s  %s",MeasurementID,measurement_instance )
            return measurement_instance
            #del(measurement_instance)
            #del(host)
            #gc.collect()
    def initMeasurments(self):
        for  host in self.hosts[int(self.startHost):int(self.lastHost)]:
            self.hostData.update({host['ID']:{}})   
        
            for MeasurementID in host['MeasurementsIDs']:
               
                try:
                    test=self.hostData[host['ID'][MeasurementID]]
                except:
                    self.hostData[host['ID']].update({ MeasurementID:{} })

                self.hostData[host['ID']][MeasurementID].update({'start':time.time()})
                self.hostData[host['ID']][MeasurementID].update({'polling_period':120})
                self.hostData[host['ID']][MeasurementID].update({'next_time':time.time()})
                
                # if self.measurementsConfig[MeasurementID]['Mode'] =='doubleindextag':
                #     logging.warning("Add doubletag measurements")
                    
                # else: 
                #   measurement_instance=measurement_Thread(self,host,MeasurementID)
                measurement_instance=measurement_Thread(self,host,MeasurementID)
                measurement_instance.setName(host['ID']+'-'+MeasurementID)
                measurement_instance.setDaemon(True)
                measurement_instance.start()
                logging.debug("Add instance to pool %s-%s  %s",host['ID'],MeasurementID,measurement_instance )
                self.RunningMeasurement.add(measurement_instance)
                self.hostData[host['ID']][MeasurementID].update({'instance':measurement_instance})
                #del(measurement_instance)
                #del(host)
                #gc.collect()
        
    #@profile(precision=4)
    def run2(self):
            #for measurement_instance in self.RunningMeasurement:
            #    del(measurement_instance)
                
            #del(self.RunningMeasurement)
            proc = psutil.Process()            
            
            if len(self.RunningMeasurement) >0:
                logging.error("Run host measurements: %s %s:%s" ,len(self.hosts) ,self.startHost,self.lastHost)
                for measurement_instance in self.RunningMeasurement:
                    start=time.time() 
                    if measurement_instance.is_alive():
                        if proc.num_fds() >int(self.config['base']['fd_limit_low']):
                            time.sleep(0.1)
                        if proc.num_fds() >int(self.config['base']['fd_limit_high']):
                            wait=time.time() 
                            while proc.num_fds() >int(self.config['base']['fd_limit_low'])-100:
                                time.sleep(0.1)
                            print("Többet Várunk",time.time()-wait, proc.num_fds())
                        measurement_instance.setEnable()
                        measurement_instance.SetEnableDataGather()
                        time.sleep(0.2) # workaround not start the all measurment same time
                        print('Running measurments',len(self.RunningMeasurement),'/',self.GetRuningMeasurementsNumber(), (time.time()-start),proc.num_fds())
            #        measurement_instance.setEnable()

            else:
                logging.error("Add host measurements: %s %s:%s" ,len(self.hosts) ,self.startHost,self.lastHost)
                #print(self.hosts)
                try:
                    n=0
                    for  host in self.hosts[int(self.startHost):int(self.lastHost)]:
                        start=time.time() 
                        if host['Enable']==True:
                            #https://github.com/kamakazikamikaze/easysnmp/issues/119
                            # Use snmp_sess_select_info2() for processing large file descriptors #119 
                            # Dynamic wait 
                            if proc.num_fds() >int(self.config['base']['fd_limit_low']):
                                time.sleep(0.05)
                            if proc.num_fds() >int(self.config['base']['fd_limit_high']):
                                wait=time.time()
                                while proc.num_fds() >int(self.config['base']['fd_limit_low'])-100:
                                    time.sleep(0.1)
                                print("Többet Várunk",time.time()-wait, proc.num_fds())
                            
                            self.RunningMeasurement.add(self.startMeasurments(host))
                            print('Starting measurments',len(self.RunningMeasurement),'/',self.GetRuningMeasurementsNumber(), (time.time()-start),proc.num_fds())
                            #file=self.config['logging']['logdir']+'/json_dump/{}-{}.json'.format( host['ID'], 'processing' )
                            #self.dumpjson(file,self.hostData[host['ID']])

                           
                except Exception as s:
                    logging.error("Measurment hosts error %s  (%s : %s ) ", s,self.startHost,self.lastHost )
                    return    
            # logging.warning("Start %s instances ", len(self.RunningMeasurement))
            # for host_instance in self.RunningMeasurement:
            #     logging.warning("Enable host measurement thread %s -- %s",host_instance.host_data['ID'], host_instance.MeasurementID)
            #     host_instance.setEnable()
            #Thread.active_count()
            
            logging.warning("Runing hosts %s %s", len(self.RunningMeasurement), '')
            try:
                print('Running measurments',len(self.RunningMeasurement),self.GetRuningMeasurementsNumber(),self.GetDiedMeasurementsNumber())
            except Exception as s:
                pass
            gc.collect()

    def GetRuningMeasurementsNumber(self):
        RuningMeasurementsNumber=0
        for measurement_instance in self.RunningMeasurement:
            if measurement_instance.isRunning:
                RuningMeasurementsNumber += 1
        return RuningMeasurementsNumber


    def GetDiedMeasurementsNumber(self):
        DiedMeasurementsNumber=0
        for measurement_instance in self.RunningMeasurement:
            if not measurement_instance.is_alive():
                DiedMeasurementsNumber += 1
        return DiedMeasurementsNumber


    def dumpjson(self,filename,data):
        try:
            with open(filename, 'w') as outfile:
                json.dump(data, outfile)
        except Exception as s:
            logging.error('Json file dump error'+ str(s))
    
    def loadjson(self,filename):
        try:
            with open(filename, 'r') as file:
               return json.load(file)
        except Exception as s:
            logging.error('Json file load error'+ str(s))

    def run3(self):
            #for measurement_instance in self.RunningMeasurement:
            #    del(measurement_instance)
                
            #del(self.RunningMeasurement)
            proc = psutil.Process()            
           
            logging.error("Run host measurements: %s %s:%s" ,len(self.hosts) ,self.startHost,self.lastHost)
            #print(self.hostData)
            for hostID in self.hostData:
                for MeasurementID in self.hostData[hostID]:
                    measurment=self.hostData[hostID][MeasurementID]
                    start=time.time() 
                    if measurment['instance'].is_alive():
                        print(hostID, MeasurementID, start , measurment['next_time'])
                        if start > measurment['next_time']:
                            if proc.num_fds() >int(self.config['base']['fd_limit_low']):
                                time.sleep(0.1)
                            if proc.num_fds() >int(self.config['base']['fd_limit_high']):
                                wait=time.time() 
                                while proc.num_fds() >int(self.config['base']['fd_limit_low'])-100:
                                    time.sleep(0.1)
                                print("Többet Várunk",time.time()-wait, proc.num_fds())
                            measurment['instance'].setEnable()
                            measurment['instance'].SetEnableDataGather()
                            time.sleep(0.2) # workaround not start the all measurment same time
                            print('Running measurments',len(self.RunningMeasurement),'/',self.GetRuningMeasurementsNumber(), (time.time()-start),proc.num_fds())
                            self.hostData[hostID][MeasurementID].update({'start':time.time()})
                            self.hostData[hostID][MeasurementID].update({'next_time':time.time()+measurment['polling_period']})
                file=self.config['logging']['logdir']+'/json_dump/{}-{}.json'.format( hostID, 'processing' )
                self.dumpjson(file,self.hostData[hostID])
        #        measurement_instance.setEnable()


    def run(self):
        #self.statistic.setdata(self.statistic.data + ' host: ' +str(len(self.hosts) ))
        #time.sleep(randint(1,100))
        logging.error("Run host measurements: %s %s:%s" ,len(self.hosts) ,self.startHost,self.lastHost)
        proc = psutil.Process()
        self.initMeasurments()
        while True:
            start=time.time() 
            self.state='run'
            #if  self.GetRuningMeasurementsNumber() ==0:
            #    logging.error("Died measurements number %s %s", self.GetDiedMeasurementsNumber(), '')
                #self.run3()
            #else:
            #    logging.warning("Skip run high process number %s %s", proc.num_fds(), '')
            #print('Round time',time.time()-start, proc.num_fds())
            logging.debug("SnmpHosts range %s-%s   measurments runing /died  %s / %s -- ",self.startHost, self.lastHost,self.GetRuningMeasurementsNumber(), self.GetDiedMeasurementsNumber() )
            for hostID in self.hostData:
                for MeasurementID in self.hostData[hostID]:
                    measurment=self.hostData[hostID][MeasurementID]
                    start=time.time() 
                    if measurment['instance'].is_alive():
                        logging.debug("%s-%s is alive",hostID, MeasurementID)
                    else:
                        logging.debug("%s-%s is died",hostID, MeasurementID)

           # while proc.num_fds() >500:
           #mem2 = proc.get_memory_info().rss
            #     time.sleep(1)
            #     print("A végén Többet Várunk",proc.num_fds())
    
            time.sleep(10)
            
            #for measurement_instance in self.RunningMeasurement:
            #    if measurement_instance.is_alive():
            #        measurement_instance.Setkill()
                #del(measurement_instance)
            #del(self.RunningMeasurement)

            #print(self.hostData)

            

   