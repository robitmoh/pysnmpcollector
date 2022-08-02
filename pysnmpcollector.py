from memory_profiler import profile
from threading import Thread
from threading import Semaphore
#from collections import namedtuple as NT



import logging
import time
from datetime import datetime
from datetime import timedelta
from pympler import asizeof

import json
import gc
# See SNMP.py module at the bottom of the question
#from snmp import v2c


from pysnmp.entity.rfc3413 import mibvar
from pysnmp.proto import rfc1902
from pysnmp.proto import rfc1905
from pysnmp.smi import builder, view, error



from itertools import islice 

import configparser
import argparse

from snmphosts import SnmpHosts
import pprint
import os

class Statistic(object):
    def __init__(self, config):
        super(Statistic, self).__init__()
        self.config=config
        self.data="statistic data"
        self.ActiveMeasurment=0
        self.TimeoutedMeasurment=0
        self.RunningMeasurment=0
        self.AvgMeasurmentTime=0
    
    def IncActiveMeasurment(self):
        self.ActiveMeasurment=self.ActiveMeasurment+1
    def IncTimeoutedMeasurment(self):
        self.TimeoutedMeasurment=self.TimeoutedMeasurment+1   
    def IncRunningMeasurment(self):
        self.RunningMeasurment=self.RunningMeasurment+1
    def DecRunningMeasurment(self):
        self.RunningMeasurment=self.RunningMeasurment-1
    def AddMeasurmentTime(self,value):
        self.AvgMeasurmentTime=self.AvgMeasurmentTime+value

    def setdata(self,value):
        self.data=value
        
    def print_statistic(self):
        try:
            print(self.data, self.ActiveMeasurment, self.TimeoutedMeasurment, self.RunningMeasurment, self.AvgMeasurmentTime/self.ActiveMeasurment)
        except:
            pass

if __name__=='__main__':

    parser = argparse.ArgumentParser()

    #subparsers = parser.add_subparsers()
    #parser_base = subparsers.add_parser('base')
    parser.add_argument('-F','--hostfile', type=str, dest='default_host_file')
    parser.add_argument('-M','--process_mode', type=str, dest='process_mode')

    #parser_log = subparsers.add_parser('logging')
    parser.add_argument('-T','--this_subprocess', dest='this_subprocess')
    parser.add_argument('-S','--sub_start', dest='sub_start')
    parser.add_argument('-E','--sub_end', dest='sub_end')


    config = configparser.ConfigParser()
    config.read('./cfg/pysnmpcollector.cfg')

    try:
        os.makedirs(config['base']['tmp_dir']+"/json_dump/")
    except FileExistsError:
        # directory already exists
        pass
    
    try:
        os.makedirs(config['logging']['logdir']+"/host_logs/")
    except FileExistsError:
        # directory already exists
        pass
    



    #for subp, subn in ((parser_base, "base"), (parser_log, "logging")):
    #    subp.set_defaults(**dict(config.items(subn)))

    #print(subp.parse_args())
    args=parser.parse_args()
    #print(args)

    #print (config['logging']['level'])
    #print (parser.parse_args(['logging', '-L', 'www.example.com']))
    
    if args.this_subprocess=='y':
        filename=config['logging']['logdir']+config['logging']['logfile']  #+'-'+str(args.sub_end)
    else:
        filename=config['logging']['logdir']+config['logging']['logfile']
    logging.basicConfig(level=config['logging']['file_level'].upper(), format='[%(asctime)s] [%(levelname)s] [%(process)d] [%(threadName)s] %(message)s', 
        filename=filename, filemode='w')
    console = logging.StreamHandler()
    # optional, set the logging level
    console.setLevel(config['logging']['console_level'].upper())
    # set a format which is the same for console use
    formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(process)d] [%(threadName)s] %(message)s')
    # tell the handler to use this format
    console.setFormatter(formatter)
    # add the handler to the root logger
    logging.getLogger('').addHandler(console) 
    #logging.propagate= False
    #exit
    #SnmpHosts=SnmpHosts()
    #SnmpHosts.run()

    mib_dir_root = "./mibs"
    mib_dirs = [] #'Force10', 'Cisco', 'Dell'
    mib_dirs = [os.path.join(mib_dir_root, d) for d in mib_dirs if os.path.isdir(os.path.join(mib_dir_root, d))]
    os.environ['MIBDIRS'] = "+{:s}".format(";".join(mib_dirs))
    #print(os.environ['MIBDIRS'])
    
    

    
    host_file=config['base']['default_host_file']
    with open(host_file) as json_file:
        hosts=json.load(json_file)
    
    snmpHosts = set()
    
    ############   Single Process
    if config['base']['process_mode'].lower() == 'singleprocess':
        statistic=Statistic(config)
        from multiprocessing import Event
        event = Event()
        Snmphost=SnmpHosts(event,config,statistic,hosts)
        Snmphost.start()
        snmpHosts.add(Snmphost)
   
    #  MultiProcess        
   ###################################################################################xxx
    
    if config['base']['process_mode'].lower() == 'multiprocess':
        from multiprocessing import Pool, cpu_count, Event
        from multiprocessing import Process, Manager
        from multiprocessing.managers import BaseManager 
        import multiprocessing_logging

        multiprocessing_logging.install_mp_handler()
        event = Event()
        BaseManager.register('Statistic', Statistic)
        manager = BaseManager()
        manager.start()
        #inst = manager.SimpleClass()
        statistic = manager.Statistic(config)
        def host_pool( process_host):   
            print(process_host)
            Snmphost=SnmpHosts(event,config,statistic, hosts)
            for key,value in process_host.items():
                Snmphost.startHost=key
                Snmphost.lastHost=value
            Snmphost.start()
            snmpHosts.add(Snmphost)

            return ''
        try:
            workers = int(config['base']['fork'])
            #cpu_count()*2
        except NotImplementedError:
            workers = 1
        process_host=[]
        print(workers)
        pool = Pool(processes=workers)
        pre=0
        host_chunk=[]
        procnum=int(len(hosts)/workers)
        if procnum ==0 :
            procnum=1
        for i in range(0,len(hosts),procnum+1):
            last=i+procnum
            if last>len(hosts):
                last=len(hosts)
            print(len(hosts),i,last)
            host_chunk.append({i:last})

        results = pool.map(host_pool, host_chunk)
    
    if config['base']['process_mode'].lower() == 'subprocess':
        import subprocess
        from multiprocessing import Pool, cpu_count, Event
        event = Event()
        
        if args.this_subprocess =='y':
            logging.error("Start Subprocess " + str(args.sub_start) +"-" + str(args.sub_end))
            statistic=Statistic(config)
            Snmphost=SnmpHosts(event,config,statistic,hosts)
            Snmphost.startHost=args.sub_start
            Snmphost.lastHost=args.sub_end
            Snmphost.start()
            snmpHosts.add(Snmphost)

        else: # Coordinator process
            host_chunk=[]

            workers = int(config['base']['fork'])
            procnum=int(len(hosts)/workers)
            if procnum ==0 :
                procnum=1
            print(procnum)
            for i in range(0,len(hosts),procnum):
                last=i+procnum
                if last>len(hosts):
                    last=len(hosts)
                print(len(hosts),i,last)
                cmd = ["python3", "pysnmpcollector.py", "--this_subprocess=y", "--sub_start="+str(i) ,"--sub_end="+str(last) ]
                logging.error("Start Subprocess " +cmd[2])
                child = subprocess.Popen( cmd, shell=False )
                snmpHosts.add(child)

    while True:
        time.sleep(60)
        print('main')
        statistic.print_statistic()
        print('Main Self', asizeof.asizeof(snmpHosts))
        try:
            for process in snmpHosts:
                print(process)
                print('Process size', asizeof.asizeof(process))

                #process.RuningHosts=set()
                #gc.collect()
                print('Process size', asizeof.asizeof(process))

        except KeyboardInterrupt:
            event.set()
            break
        


        
    
    
    #results.close()
    #results.join()