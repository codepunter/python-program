import logging, logging.handlers
import locale
import cherrypy
import os
from elementtree import ElementTree


"""--- Read the conf.XML to read the Report data---"""
path = os.environ['RAD_INSTALL_DIR']
path = os.path.join(path, "conf", "webappconfig.xml")
config = {}
locale.setlocale(locale.LC_ALL, '')
log = logging.getLogger("")  #root logger
logging.raiseExceptions = 0
from SingletonPattern import SingletonType


class logconf:
    __metaclass__ = SingletonType
    def __init__(self, logfilename=None, logport=logging.handlers.DEFAULT_TCP_LOGGING_PORT):
        try:
            log.info("Initializing logging...")
            et = ElementTree.parse(path)
            root = et.getroot()
            for i in root:
                config[i.tag] = dict((k.tag, k.text) for k in i)
                ##in case we want to go for Log Level
            #logger.setLevel(logging.DEBUG)  
            log_lvl = config['WEBLOGS']['LOGLEVEL']
            if log_lvl == 'INFO':
                log.setLevel(logging.INFO)
            elif log_lvl == 'CRITICAL':
                log.setLevel(logging.CRITICAL)
            else:
                log.setLevel(logging.DEBUG)
            if not logfilename:
                log.info("Init Network logging")
                socket_handler = logging.handlers.SocketHandler('localhost', logport)
                if not log.handlers:
                    log.addHandler(socket_handler)
            else:
                log.info("Init file logging")
                logfile = os.path.join(os.getenv("RAD_INSTALL_DIR", "."), cherrypy.config.get('server.logFile', os.path.join('log',logfilename)))
                hdlr = logging.FileHandler(logfile, "a")
                fmt = logging.Formatter("%(asctime)s-[%(levelname)-5s] %(message)s [%(filename)s]-[%(lineno)4d]", "%x %X")
                hdlr.setFormatter(fmt)
                if not log.handlers:
                    log.addHandler(hdlr)
            log.info("Initialized logging... :%s", log.level)
        except Exception, e:
            log.info("== == Error in Reading conf.xml - - %s", str(e))
            print str(e)
