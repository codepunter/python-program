#!/usr/bin/env python
from turbogears import config, update_config, start_server

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

cherrypy.config.update(os.path.join(os.getenv("RAD_INSTALL_DIR"),"conf","MS_prod.cfg"))
update_config(configfile="MS_prod.cfg",modulename="radspeed.config")

from radspeed.core.logconf import *
logconf("InstaMSWordReporting.log")

class MSWordProcess():
    def __init__(self, port):
        self.port = port
        self.tg_init()
    
    def tg_init(self):
        try:
            from radspeed.MSWordReportController import MSWordReportController, Root
            Root.MSWORD = MSWordReportController()
    
            from radspeed import mysession
            cherrypy.lib.sessions.MysqlSession = mysession.RadspeedSQLStorage
    
            # Set environment to production to disable auto-reload.
            cherrypy.config.update({'global': {'server.environment': 'production',
                                               'server.socket_port':self.port,'engine.autoreload_on':False,
                                               'i18n.get_locale' : lambda: locale},})
                
            self.root = Root
            start_server(Root())
        except Exception,e:
            print "<<start-mswordserver -- tg_init>> Exception : "+str(e)
            log.critical("<<start-mswordserver -- tg_init>> Error occured while initializing the ms word server : %s. Shutting down the service.",e)
            # Shall stop service
            win32serviceutil.StopService('InstaMSWebService')
            
if __name__ == "__main__":
    port = int(sys.argv[1])
    #log.info("Starting ms word server process on port : %s",port)
    mswordProcess = MSWordProcess(port)
    #log.info("Stopping web process")