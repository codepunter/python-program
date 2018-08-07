import cherrypy
import win32serviceutil
import win32service
import win32event
import win32api
import win32process
import os

# BUILT IN MODULE
import time
import datetime
import string

from turbogears import config, update_config, start_server
cherrypy.lowercase_api = True
from os.path import *
os.environ['WEB_PROCESS'] = '1'
import sys
from radspeed.core.logconf import *
from radspeed import messageQueue

import logging
log = logging.getLogger(__name__)

logconf("messageSyncingLog.log")
log.setLevel(logging.INFO)

runProcess = True
# reading config parameter to enble Dicom Edit feature


    
class MyService(win32serviceutil.ServiceFramework):
    """NT Service."""
    
    _svc_name_ = "InstaMessageSyncService"
    _svc_display_name_ = "InstaMessageSyncService"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        # create an event that SvcDoRun can wait on and SvcStop
        # can set.
        self.stop_event = win32event.CreateEvent(None, 0, 0, None)

    def SvcDoRun(self):
        global runProcess   
   
    
	try:
	    messageQueue.start()
	    #messageQueue.startReportStatus()
	except Exception,e:
	    log.critical("*****************************************")
	    log.critical("Failed to start Insta MessageSync because : %s",e)
	    log.critical("*****************************************")
	    self.SvcStop()
	win32event.WaitForSingleObject(self.stop_event, win32event.INFINITE)    
    def SvcStop(self):
        runProcess = False
	# Now Stop cherrypy server
	
	#if str(controllers.shouldStartEventConsumer)!="0":
	#    controllers.globalProcess.stopEventConsumer()
        #cherrypy.server.stop()
	self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.stop_event)

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(MyService)
