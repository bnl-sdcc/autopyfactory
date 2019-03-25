#! /usr/bin/env python

__author__ = "Graeme Andrew Stewart, John Hover, Jose Caballero"
__copyright__ = "2007,2008,2009,2010 Graeme Andrew Stewart; 2010-2017 John Hover; 2010-2017 Jose Caballero"
__credits__ = []
__license__ = "Apache 2.0"
__version__ = "2.4.16"
__maintainer__ = "Jose Caballero"
__email__ = "jcaballero@bnl.gov,jhover@bnl.gov"
__status__ = "Production"

"""
    Main module for autopyfactory. 
"""

import datetime
import logging
import logging.handlers
import threading
import time
import traceback
import os
import platform
import pwd
import smtplib
import socket
import sys

from optparse import OptionParser
from pprint import pprint
from ConfigParser import ConfigParser
from Queue import Queue

try:
    from email.mime.text import MIMEText
except:
    from email.MIMEText import MIMEText

import pluginmanager

from autopyfactory.apfexceptions import FactoryConfigurationFailure, PandaStatusFailure, ConfigFailure
from autopyfactory.apfexceptions import CondorVersionFailure, CondorStatusFailure
from autopyfactory.apfexceptions import ThreadRegistryInvalidKind
from autopyfactory.authmanager import AuthManager
from autopyfactory.cleanlogs import CleanLogs
from autopyfactory.config import ConfigHandler
from autopyfactory.configloader import Config, ConfigManager
from autopyfactory.logserver import LogServer
from autopyfactory.queues import APFQueuesManager
from autopyfactory.threadmanagement import ThreadsRegistry

major, minor, release, st, num = sys.version_info


class FactoryCLI(object):
    """class to handle the command line invocation of APF. 
       parse the input options,
       setup everything, and run Factory class
    """
    def __init__(self):
        self.options = None 
        self.args = None
        self.log = None
        self.fcl = None

        self.__presetups()
        self.__parseopts()
        self.__setuplogging()
        self.__platforminfo()
        self.__checkroot()
        self.__createconfig()


    def __presetups(self):
        """
        we put here some preliminary steps that 
        for one reason or another 
        must be done before anything else
        """

    
    def __parseopts(self):
        parser = OptionParser(usage="""%prog [OPTIONS]
autopyfactory is an ATLAS pilot factory.

This program is licenced under the GPL, as set out in LICENSE file.

Author(s):
Graeme A Stewart <g.stewart@physics.gla.ac.uk>
Peter Love <p.love@lancaster.ac.uk>
John Hover <jhover@bnl.gov>
Jose Caballero <jcaballero@bnl.gov>
""", version="%prog $Id: factory.py 7680 2011-04-07 23:58:06Z jhover $" )


        parser.add_option("-d", "--debug", 
                          dest="logLevel", 
                          default=logging.WARNING,
                          action="store_const", 
                          const=logging.DEBUG, 
                          help="Set logging level to DEBUG [default WARNING]")
        parser.add_option("-v", "--info", 
                          dest="logLevel", 
                          default=logging.WARNING,
                          action="store_const", 
                          const=logging.INFO, 
                          help="Set logging level to INFO [default WARNING]")
        parser.add_option("--console", 
                          dest="console", 
                          default=False,
                          action="store_true", 
                          help="Forces debug and info messages to be sent to the console")
        parser.add_option("--quiet", dest="logLevel", 
                          default=logging.WARNING,
                          action="store_const", 
                          const=logging.WARNING, 
                          help="Set logging level to WARNING [default]")
        parser.add_option("--oneshot", "--one-shot", 
                          dest="cyclesToDo", 
                          default=0,
                          action="store_const", 
                          const=1, 
                          help="Run one cycle only")
        parser.add_option("--cycles", 
                          dest="cyclesToDo",
                          action="store", 
                          type="int", 
                          metavar="CYCLES", 
                          help="Run CYCLES times, then exit [default infinite]")
        parser.add_option("--sleep", dest="sleepTime", 
                          default=120,
                          action="store", 
                          type="int", 
                          metavar="TIME", 
                          help="Sleep TIME seconds between cycles [default %default]")
        parser.add_option("--conf", dest="confFiles", 
                          default="/etc/autopyfactory/autofactory.conf",
                          action="store", 
                          metavar="FILE1[,FILE2,FILE3]", 
                          help="Load configuration from FILEs (comma separated list)")
        parser.add_option("--log", dest="logfile", 
                          metavar="LOGFILE", 
                          action="store",
                          default="stdout", 
                          help="Send logging output to LOGFILE or SYSLOG or stdout [default <syslog>]")
        parser.add_option("--runas", dest="runAs", 
                          #
                          # By default
                          #
                          default=pwd.getpwuid(os.getuid())[0],
                          action="store", 
                          metavar="USERNAME", 
                          help="If run as root, drop privileges to USER")
        (self.options, self.args) = parser.parse_args()

        #self.options.confFiles = self.options.confFiles.split(',')

    def __setuplogging(self):
        """ 
        Setup logging 
        
        General principles we have tried to used for logging: 
        
        -- Logging syntax and semantics should be uniform throughout the program,  
           based on whatever organization scheme is appropriate.  
        
        -- Have sufficient DEBUG messages to show domain problem calculations input and output.
           DEBUG messages should never span more than one line. 
        
        -- A moderate number of INFO messages should be logged to mark major  
           functional steps in the operation of the program,  
           e.g. when a persistent object is instantiated and initialized,  
           when a functional cycle/loop is complete.  
           It would be good if these messages note summary statistics,  
           e.g. "the last submit cycle submitted 90 jobs and 10 jobs finished".  
           A program being run with INFO log level should provide enough output  
           that the user can watch the program function and quickly observe interesting events. 
        
        -- Initially, all logging should be directed to a single file.  
           But provision should be made for eventually directing logging output from different subsystems  
           (submit, info, proxy management) to different files,  
           and at different levels of verbosity (DEBUG, INFO, WARN), and with different formatters.  
           Control of this distribution should use the standard Python "logging.conf" format file: 
        
        -- All messages are always printed out in the logs files, 
           but also to the stderr when DEBUG or INFO levels are selected. 
        
        -- We keep the original python levels meaning,  
           including WARNING as being the default level.  
        
                DEBUG      Detailed domain problem information related to scheduling, calculations,
                           program state.  
                INFO       High level confirmation that things are working as expected.  
                WARNING    An indication that something unexpected happened,  
                           or indicative of some problem in the near future (e.g. 'disk space low').  
                           The software is still working as expected. 
                ERROR      Due to a more serious problem, the software has not been able to perform some function. 
                CRITICAL   A serious error, indicating that the program itself may be unable to continue running. 
        
        """

        self.log = logging.getLogger()

        self.options.logfile = os.path.expanduser(self.options.logfile)
        if self.options.logfile == 'syslog':
            logStream = logging.handlers.SysLogHandler('/dev/log')
        elif self.options.logfile == 'stdout':
            logStream = logging.StreamHandler()
        else:
            lf = os.path.expanduser(self.options.logfile)
            logdir = os.path.dirname(lf)
            if not os.path.exists(logdir):
                os.makedirs(logdir)
            runuid = pwd.getpwnam(self.options.runAs).pw_uid
            rungid = pwd.getpwnam(self.options.runAs).pw_gid                  
            os.chown(logdir, runuid, rungid)
            logStream = logging.FileHandler(filename=lf)    

        if major == 2 and minor == 4:
            FORMAT='%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d : %(message)s'
        else:
            FORMAT='%(asctime)s (UTC) [ %(levelname)s ] %(name)s %(filename)s:%(lineno)d %(funcName)s(): %(message)s'
        formatter = logging.Formatter(FORMAT)
        formatter.converter = time.gmtime  # to convert timestamps to UTC
        logStream.setFormatter(formatter)
        self.log.addHandler(logStream)

        # adding a new Handler for the console, 
        # to be used only for DEBUG and INFO modes. 
        if self.options.logLevel in [logging.DEBUG, logging.INFO]:
            if self.options.console:
                console = logging.StreamHandler(sys.stdout)
                console.setFormatter(formatter)
                console.setLevel(self.options.logLevel)
                self.log.addHandler(console)
        self.log.setLevel(self.options.logLevel)
        self.log.info('Logging initialized.')


    def _printenv(self):

        envmsg = ''        
        for k in sorted(os.environ.keys()):
            envmsg += '\n%s=%s' %(k, os.environ[k])
        self.log.debug('Environment : %s' %envmsg)


    def __platforminfo(self):
        """
        display basic info about the platform, for debugging purposes 
        """
        self.log.info('platform: uname = %s %s %s %s %s %s' %platform.uname())
        self.log.info('platform: platform = %s' %platform.platform())
        self.log.info('platform: python version = %s' %platform.python_version())
        self._printenv()

    def __checkroot(self): 
        """
        If running as root, drop privileges to --runas' account.
        """
        starting_uid = os.getuid()
        starting_gid = os.getgid()
        starting_uid_name = pwd.getpwuid(starting_uid)[0]

        hostname = socket.gethostname()
        
        if os.getuid() != 0:
            self.log.info("Already running as unprivileged user %s at %s" % (starting_uid_name, hostname))
            
        if os.getuid() == 0:
            try:
                runuid = pwd.getpwnam(self.options.runAs).pw_uid
                rungid = pwd.getpwnam(self.options.runAs).pw_gid
                os.chown(self.options.logfile, runuid, rungid)
                
                os.setgid(rungid)
                os.setuid(runuid)
                os.seteuid(runuid)
                os.setegid(rungid)

                self._changehome()
                self._changewd()

                self.log.info("Now running as user %d:%d at %s..." % (runuid, rungid, hostname))
                self._printenv()

            
            except KeyError as e:
                self.log.error('No such user %s, unable run properly. Error: %s' % (self.options.runAs, e))
                sys.exit(1)
                
            except OSError as e:
                self.log.error('Could not set user or group id to %s:%s. Error: %s' % (runuid, rungid, e))
                sys.exit(1)

    def _changehome(self):
        """
        at some point, proxyManager will make use of method
              os.path.expanduser()
        to find out the absolute path of the usercert and userkey files
        in order to renew proxy.   
        The thing is that expanduser() uses the value of $HOME
        as it is stored in os.environ, and that value still is /root/
        Ergo, if we want the path to be expanded to a different user, i.e. autopyfactory,
        we need to change by hand the value of $HOME in the environment
        """
        runAs_home = pwd.getpwnam(self.options.runAs).pw_dir 
        os.environ['HOME'] = runAs_home
        self.log.debug('Setting up environment variable HOME to %s' %runAs_home)


    def _changewd(self):
        """
        changing working directory to the HOME directory of the new user,
        typically "autopyfactory". 
        When APF starts as a daemon, working directory may be "/".
        If APF was called from the command line as root, working directory is "/root".
        It is better is current working directory is just the HOME of the running user,
        so it is easier to debug in case of failures.
        """
        runAs_home = pwd.getpwnam(self.options.runAs).pw_dir
        os.chdir(runAs_home)
        self.log.debug('Switching working directory to %s' %runAs_home)


    def __createconfig(self):
        """Create config, add in options...
        """
        if self.options.confFiles != None:
            try:
                self.fcl = ConfigManager().getConfig(self.options.confFiles)
            except ConfigFailure:
                self.log.error('Failed to create FactoryConfigLoader')
                sys.exit(1)
        
        self.fcl.set("Factory","cyclesToDo", str(self.options.cyclesToDo))
        self.fcl.set("Factory", "sleepTime", str(self.options.sleepTime))
        self.fcl.set("Factory", "confFiles", self.options.confFiles)
           
    def run(self):
        """Create Factory and enter main loop
        """

        from autopyfactory.factory import Factory

        try:
            self.log.info('Creating Factory and entering main loop...')

            f = Factory(self.fcl)
            f.run()
            
        except KeyboardInterrupt:
            self.log.info('Caught keyboard interrupt - exitting')
            f.stop()
            sys.exit(0)
        except FactoryConfigurationFailure as e:
            self.log.error('Factory configuration failure: %s', e)
            sys.exit(1)
        except ImportError as e:
            self.log.error('Failed to import necessary python module: %s' % e)
            sys.exit(1)
        except:
            # TODO - make this a logger.exception() call
            self.log.error("""Please report to Jose <jcaballero@bnl.gov> and John <jhover@bnl.gov>.""")
            # The following line prints the exception to the logging module
            self.log.error(traceback.format_exc(None))
            print(traceback.format_exc(None))
            sys.exit(1)          
          

class Factory(object):
    """
    -----------------------------------------------------------------------
    Class implementing the main loop. 
    The class has two main goals:
            1. load the config files
            2. launch a new thread per queue 

    Information about queues created and running is stored in a 
    APFQueuesManager object.

    Actions are triggered by method update() 
    update() can be invoked at the beginning, from __init__,
    or when needed. For example, is an external SIGNAL is received.
    When it happens, update() does:
            1. calculates the new list of queues from the config file
            2. updates the APFQueuesManager object 
    -----------------------------------------------------------------------
    Public Interface:
            __init__(fcl)
            mainLoop()
            update()
    -----------------------------------------------------------------------
    """

    @staticmethod
    def getFactoryMock(fcl, am):
        """
        creates and returns a Mock class for Factory
        A Factory Mock only needs the factory config loader (fcl)
        the authmanager (am), the ThreadsRegistry, and a faked monitor config loader (mcl)
        """

        from autopyfactory.threadsmanagement import ThreadsRegistry

        class FactoryMock(object):
            def __init__(self, fcl, am):
                self.fcl = fcl
                self.mcl = Config()
                self.mcl.add_section('MockMonitor')
                self.mcl.set('MockMonitor','monitorURL','')
                self.threadsregistry = ThreadsRegistry()
                self.authmanager = am

        return FactoryMock(fcl, am)



    def __init__(self, fcl):
        """
        fcl is a FactoryConfigLoader object. 
        """
        self.version = __version__
        self.log = logging.getLogger('autopyfactory')
        self.log.info('AutoPyFactory version %s' %self.version)
        self.fcl = fcl

        # to decide if the main loop needs to stop or not
        self.shutdown = False

        # threads registry
        self.threadsregistry = ThreadsRegistry()

        # APF Queues Manager 
        self.apfqueuesmanager = APFQueuesManager(self)

        self._authmanager()
        self._queues_monitor_conf()
        self._mappings()

        # Handle Log Serving
        self._initLogserver()

        # Collect other factory attibutes
        self.adminemail = self.fcl.get('Factory','factoryAdminEmail')
        self.factoryid = self.fcl.get('Factory','factoryId')
        self.smtpserver = self.fcl.get('Factory','factorySMTPServer')
        miners = self.fcl.generic_get('Factory','factoryMinEmailRepeatSeconds', get_function='getint', default_value= 3600)
        self.minemailrepeat = int(miners)
        # Dictionary of emails sent, indexed by subject + message, with value
        # being a Date object of when it was sent. 
        self.emaildict = {}
        self.hostname = socket.gethostname()
        self.username = pwd.getpwuid(os.getuid()).pw_name   
        # to shutdown the factory when there are no active queues
        self.abort_no_queues = self.fcl.generic_get('Factory', 'abort_no_queues', get_function='getboolean', default_value=False)

        # the the queues config loader object, to be filled by a Config plugin
        self.qcl = Config()

        self._plugins()

        # Log some info...
        self.log.debug('Factory shell PATH: %s' % os.getenv('PATH') )     
        self.log.info("Factory: Object initialized.")


    def _authmanager(self):

        # Handle ProxyManager configuration
        try:
            useaman = self.fcl.getboolean('Factory', 'authmanager.enabled')
            self.log.debug("Authmanager enabled in config.")
        except Exception:
            self.log.error('No authmanager var in config. Skipping. ')
        
        if useaman:      
            try:
                from autopyfactory.authmanager import AuthManager
            except Exception:
                self.log.exception('authmanager cannot be imported')
            self.authmanager = AuthManager(factory=self)
            self.log.info('AuthManager initialized.')
        else:
            self.log.info("AuthManager disabled.")

    def _queues_monitor_conf(self):

        # Handle monitor configuration
        self.mcl = None
        self.mcf = self.fcl.generic_get('Factory', 'monitorConf')
        self.log.debug("monitor.conf file(s) = %s" % self.mcf)
        
        try:
            self.mcl = ConfigManager().getConfig(self.mcf)
        except ConfigFailure:
            self.log.error('Failed to create MonitorConfigLoader')
            sys.exit(0)

        self.log.debug("mcl is %s" % self.mcl)
       

    def _mappings(self):

        # Handle mappings configuration
        self.mappingscl = None      # mappings config loader object
        self.mappingscf = self.fcl.generic_get('Factory', 'mappingsConf') 
        self.log.debug("mappings.conf file(s) = %s" % self.mappingscf)

        try:
            self.mappingscl = ConfigManager().getConfig(self.mappingscf)
        except ConfigFailure:
            self.log.error('Failed to create ConfigLoader object for mappings')
            sys.exit(0)
        
        self.log.debug("mappingscl is %s" % self.mappingscl)


    def _plugins(self):
        
        queuesconfigpluginnames =  self.fcl.generic_get('Factory', 'config.queues.plugin', default_value='File')
        queuesconfigpluginnameslist = [i.strip() for i in queuesconfigpluginnames.split(',')]
        self.queues_config_plugins = pluginmanager.getpluginlist(['autopyfactory', 'plugins', 'factory', 'config', 'queues'], queuesconfigpluginnameslist, self, self.fcl, 'Factory')

        authconfigpluginnames =  self.fcl.generic_get('Factory', 'config.auth.plugin', default_value='File')
        authconfigpluginnameslist = [i.strip() for i in authconfigpluginnames.split(',')]
        self.auth_config_plugins = pluginmanager.getpluginlist(['autopyfactory', 'plugins', 'factory', 'config', 'auth'], authconfigpluginnameslist, self, self.fcl, 'Factory')
        
        self.monitor_plugins = []
        monitorpluginnames = self.fcl.generic_get('Factory', 'monitor', default_value=None)
        if monitorpluginnames is not None:
            monitorpluginnames_l = [i.strip() for i in monitorpluginnames.split(',')]
            try:
                self.monitor_plugins = pluginmanager.getpluginlist(['autopyfactory','plugins','factory','monitor'], monitorpluginnames_l, self, self.fcl, 'Factory')
            except Exception as e:
                self.log.warning(e)


    def _initLogserver(self):
        # Set up LogServer
        self.log.debug("Handling LogServer...")
        if self.fcl.generic_get('Factory', 'logserver.enabled', 'getboolean'):
            self.log.info("LogServer enabled. Initializing...")
            self.logserver = LogServer(self, self.fcl)
            self.log.info('LogServer initialized. Starting...')
            self.logserver.start()
            self.log.debug('LogServer thread started.')
        else:
            self.log.info('LogServer disabled. Not running.')
        
        
    def run(self):
        """
        Main functional loop of overall Factory. 
        Actions:
                1. Creates all queues and starts them.
                2. Wait for a termination signal, and
                   stops all queues when that happens.
        """

        self.log.debug("Starting.")
        self.log.info("Starting all Queue threads...")      
        confighandler = ConfigHandler(self)
        confighandler.setconfig()  # it calls the ConfigHandler._run() method, 
                                   # either in an infinite loop on single shot
                                   # it is in that _run() method where all 
                                   # APFQueues threads and Auth Handlers threads
                                   # are started
        self._start_monitor()
        self._cleanlogs()
        
        try:
            while not self.shutdown:
                mainsleep = int(self.fcl.get('Factory', 'factory.sleep'))
                time.sleep(mainsleep)
                self.log.debug('Checking for interrupt.')
                # check if queues are alive
                queues_alive = False
                for q in self.apfqueuesmanager.queues.values():
                    # Only threads have isAlive(). Some queue objects aren't threads. 
                    self.log.debug("Checking queue %s" % q.apfqname)
                    if isinstance(q, threading.Thread):
                        self.log.debug("Queue %s IS a thread." % q.apfqname)
                        if q.isAlive():
                            queues_alive = True   
                            break
    
                if not queues_alive:
                    # no queue is alive...
                    # check if factory should shutdown
                    self.log.info("no queue is alive")
                    if self.abort_no_queues:
                        self.log.info("shutting down the factory")
                        self.stop()
                        self.shutdown = True 
            self.log.debug('Leaving')
                                
        except Exception as ex:
            self.log.warning("Exception raised during Factory main loop run: %s." % ex)
            self.stop()
            raise ex
        self.log.debug("Leaving.")


    def _start_monitor(self):
        """
        start all factory-level monitor plugins threads
        """
        for monitor in self.monitor_plugins:
            monitor.start()


    def _cleanlogs(self):
        """
        starts the thread that will clean the condor logs files
        """
        self.log.debug('Starting')
        self.clean = CleanLogs(self, self.fcl, self.qcl)
        self.clean.start()
        self.log.debug('Leaving')


    def stop(self):
        """
        Method to cleanly shut down all factory activity, joining threads, etc. 
        """
        logging.debug(" Shutting down all factory threads...")
        self.shutdown = True
        self.threadsregistry.join() 
        self.log.debug('Leaving')
        for t in threading.enumerate():
            self.log.debug("%s" % t)

                            
    def sendAdminEmail(self, subject, messagestring):
        """
        Sends email with given subject and message to the factory admin. 
        Can be throttled by setting a minimum time between identical messages.
        
        """
        self.log.debug("Email requested. Checking for repeat time...")
        key = "%s:%s" % (subject, messagestring)
        minseconds = self.minemailrepeat
        mintd = datetime.timedelta(seconds = minseconds)
        lasttime = None
        now = datetime.datetime.now()
        try:
            lasttime = self.emaildict[key]
        except KeyError:
            pass

        if lasttime:
            tdiff = now - lasttime
            if tdiff > mintd:
                self.log.debug("Email: %s > %s" % (tdiff, mintd))
                # Send message and insert/update to dict
                self._sendEmail(subject,messagestring)
                self.emaildict[key] = now
            else:
                self.log.debug("Email: %s < %s" % (tdiff, mintd))
                self.log.info("Not sending email: %s" % messagestring)
        else:
            self.log.debug("Email: Never send before.")
            # Send message and insert/update to dict
            self._sendEmail(subject,messagestring)
            self.emaildict[key] = now

                
    def _sendEmail(self,subject,messagestring):       
        msg = MIMEText(messagestring)
        msg['Subject'] = subject
        email_from = "%s@%s" % ( self.username, self.hostname)
        msg['From'] = email_from
        msg['To'] = self.adminemail
        tolist = self.adminemail.split(",")
        
        # Send the message via our own SMTP server, but don't include the
        # envelope header.
        s = smtplib.SMTP(self.smtpserver)
        self.log.info("Sending email: %s" % msg.as_string())
        s.sendmail(email_from , tolist , msg.as_string())
        s.quit()
            
