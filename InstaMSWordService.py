#!/usr/bin/env python



import pkg_resources
pkg_resources.require("TurboGears")

import win32serviceutil
import win32service
import win32event
import win32api
import win32process
import cherrypy
import os
import turbogears
cherrypy.lowercase_api = True
from os.path import *
import sys
import StringIO
import cgitb
import traceback
os.environ['WEB_PROCESS'] = '1'

import subprocess
import xml.etree.ElementTree as ET
import shutil

from apscheduler.scheduler import Scheduler
import atexit
import httplib2

from radspeed.core.logconf import *
logconf("InstaMSWordReporting.log")

current_dir = os.path.split(__file__)[0]

tree =  ET.parse(os.path.join(os.getenv("RAD_INSTALL_DIR"),"conf","processes.xml"))
root = tree.getroot()

_python_excutable = "C:\\Python27\\python.exe"
_python_excutable_msword_server = "C:\\Python27\\python-MSWORD.exe"

if not os.path.exists(_python_excutable_msword_server):
    shutil.copy(_python_excutable,_python_excutable_msword_server)

class InstaMSWord(win32serviceutil.ServiceFramework):
    _svc_name_ = "InstaMSWebService"
    _svc_display_name_ = "InstaMSWebService"
    _svc_description_ = "InstaMSService"
    
    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)
        config_module = "radspeed.config"
        self.msword_process_id = {}
        self.sched = Scheduler(daemon=True)
        self.sched.start()
        atexit.register(lambda: self.sched.shutdown(wait=False))

    def SvcDoRun(self):
        self.ReportServiceStatus(win32service.SERVICE_START_PENDING)
        #self.tg_init()
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        
        # start cherrypy ms word server processe(s)
        processes = root.findall("./InstaMSWordProcess/Process")
        if len(processes) == 0:
            log.critical("<<InstaMSWordService : StartUp>> No MS Word process configured in process xml. Shutting down the service.")
            win32serviceutil.StopService('InstaMSWebService')
            
        for process in processes:
            port = process.attrib['port']
            self.start_msword_process(port)
        
        self.sched.add_interval_job(self.msword_process_health_check, seconds=60)
        
        #turbogears.start_server(self.root())
        win32event.WaitForSingleObject(self.stop_event, win32event.INFINITE)
    
    def start_msword_process(self, port):
        startupInfo = win32process.STARTUPINFO()
        exec_file = os.path.join(current_dir,"start-mswordserver.py")
        mswordProcessPath = os.path.join(_python_excutable_msword_server+" "+exec_file+" "+port)
        if not os.path.exists(exec_file):
            log.critical("<<start_msword_process>> Unable to start process as Process Path : %s could not be found. Going to shut down service.",exec_file)
            win32serviceutil.StopService('InstaMSWebService')
        else:
            processDetails = win32process.CreateProcess(None,mswordProcessPath, None, None, False, 0, None, None, startupInfo)
            pid=processDetails[2]
            self.msword_process_id[port]=pid
            log.info("<<start_msword_process>> Started MS Word cherrypy process: %s on port : %s", self.msword_process_id[port], port)
    
    def msword_process_health_check(self):
        try:
            processes = root.findall("./InstaMSWordProcess/Process")
            if len(processes) == 0:
                log.critical("<<msword_process_health_check>> No MS Word process found to be monitored.")
            for process in processes:
                port = process.attrib['port']
                url = "http://localhost:"+port
                #http=httplib2.Http(timeout=30)
                #headers={'Content-Type':'application/x-www-form-urlencoded'}
                #response,content = http.request(url,'GET',headers=headers)
                #if response['status'] == "200":
                #    pass
                #else:
                    #logging.critical("<<web msword_process_health_check>> MS-Word process on port : %s is not running. Going to start it again.", port)
                    #self.TerminateProcess(self.msword_process_id[port])
                    #self.start_msword_process(port)
        except IOError,e:
            log.critical("<<msword_process_health_check>> IO Error occured : %s. MS Word process on port : %s is not running. Going to start it again.",str(e),port)
            #self.start_msword_process(port)
        except Exception,e:
            log.critical("<<msword_process_health_check>> Exception occured while performing MS Word process health check. Error is : %s",str(e))
  
    def TerminateProcess(self, pid):
        try:
            PROCESS_TERMINATE = 1
            handle = win32api.OpenProcess(PROCESS_TERMINATE, False, pid)
            win32api.TerminateProcess(handle, -1)
            win32api.CloseHandle(handle)
        except Exception,e:
            logging.critical(" ** <<TerminateProcess>> Error occured while terminating ms-word process : %s. Error is : %s **",pid,e)
            
    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        #cherrypy.server.stop()
        subprocess.call(['taskkill', '/F', '/T', '/IM', 'python-MSWORD.exe'])
        self.ReportServiceStatus(win32service.SERVICE_STOPPED)

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(InstaMSWord)
