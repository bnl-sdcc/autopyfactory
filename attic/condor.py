#!/usr/bin/env python
"""
   Condor-related common utilities and library for AutoPyFactory.
   Focussed on properly processing output of condor_q -xml and condor_status -xml and converting
   to native Python data structures. 

"""
import commands
import datetime
import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
import traceback
import xml.dom.minidom

import autopyfactory.utils as utils
from autopyfactory.apfexceptions import ConfigFailure, CondorVersionFailure

from pprint import pprint
from Queue import Queue

from autopyfactory.info import JobInfo
from autopyfactory.interfaces import _thread


# FIXME !!!
# this should not be here !!!
condorrequestsqueue = Queue()


# FIXME
# factory, submitargs and wmsqueue should not be needed
def mynewsubmit(n, jsdfile, factory, wmsqueue, submitargs=None):
    """
    Submit pilots
    """
    
    log = logging.getLogger('autopyfactory')
    log.debug('Starting.')

    log.info('Attempt to submit %d pilots for queue %s' %(n, wmsqueue))

    ###     cmd = 'condor_submit -verbose '
    ###     self.log.debug('submitting using executable condor_submit from PATH=%s' %utils.which('condor_submit'))
    ###     # NOTE: -verbose is needed. 
    ###     # The output generated with -verbose is parsed by the monitor code to determine the number of jobs submitted
    ###     if self.submitargs:
    ###         cmd += self.submitargs
    ###     cmd += ' ' + jsdfile
    ###     self.log.info('command = %s' %cmd)
    ###
    ###     (exitStatus, output) = commands.getstatusoutput(cmd)
    ###     if exitStatus != 0:
    ###         self.log.error('condor_submit command for %s failed (status %d): %s', self.wmsqueue, exitStatus, output)
    ###     else:
    ###         self.log.info('condor_submit command for %s succeeded', self.wmsqueue)
    ###     st, out = exitStatus, output

    # FIXME:
    # maybe this should not be here???
    processcondorrequests = ProcessCondorRequests(factory)
    processcondorrequests.start()


    req = CondorRequest()
    req.cmd = 'condor_submit'
    args = ' -verbose '
    if submitargs:
        args += submitargs
        args += ' '
    args += ' ' + jsdfile
    req.args = args
    condorrequestsqueue.put(req)

    while not req.out:
        time.sleep(1)
    out = req.out
    st = req.rc
    if st != 0:
        log.error('condor_submit command for %s failed (status %d): %s', wmsqueue, st, out)
    else:
        log.info('condor_submit command for %s succeeded', wmsqueue)

    log.debug('Leaving with output (%s, %s).' %(st, out))
    return st, out


def parsecondorsubmit(output):
    """ 
    Parses raw output from condor_submit -verbose and returns list of JobInfo objects. 
        
    condor_submit -verbose output:
        
** Proc 769012.0:
Args = "--wrappergrid=OSG --wrapperwmsqueue=BNL_CVMFS_1 --wrapperbatchqueue=BNL_CVMFS_1-condor --wrappervo=ATLAS --wrappertarballurl=http://dev.racf.bnl.gov/dist/wrapper/wrapper-0.9.7-0.9.3.tar.gz --wrapperserverurl=http://pandaserver.cern.ch:25080/cache/pilot --wrapperloglevel=debug --script=pilot.py --libcode=pilotcode.tar.gz,pilotcode-rc.tar.gz --pilotsrcurl=http://panda.cern.ch:25880/cache -f false -m false --user managed"
BufferBlockSize = 32768
BufferSize = 524288
Cmd = "/usr/libexec/wrapper.sh"
CommittedSlotTime = 0
CommittedSuspensionTime = 0
CommittedTime = 0
CompletionDate = 0
CondorPlatform = "$CondorPlatform: X86_64-CentOS_5.8 $"
CondorVersion = "$CondorVersion: 7.9.0 Jun 19 2012 PRE-RELEASE-UWCS $"
CoreSize = 0
CumulativeSlotTime = 0
CumulativeSuspensionTime = 0
CurrentHosts = 0
CurrentTime = time()
DiskUsage = 22
EC2TagNames = "(null)"
EnteredCurrentStatus = 1345558923
Environment = "FACTORYUSER=apf APFFID=BNL-gridui08-jhover APFMON=http://apfmon.lancs.ac.uk/mon/ APFCID=769012.0 PANDA_JSID=BNL-gridui08-jhover FACTORYQUEUE=BNL_CVMFS_1-gridgk07 GTAG=http://gridui08.usatlas.bnl.gov:25880/2012-08-21/BNL_CVMFS_1-gridgk07/769012.0.out"
Err = "/home/apf/factory/logs/2012-08-21/BNL_CVMFS_1-gridgk07//769012.0.err"
ExecutableSize = 22
ExitBySignal = false
ExitStatus = 0
GlobusResubmit = false
GlobusRSL = "(jobtype=single)(queue=cvmfs)"
GlobusStatus = 32
GridResource = "gt5 gridgk07.racf.bnl.gov/jobmanager-condor"
ImageSize = 22
In = "/dev/null"
Iwd = "/home/apf/factory/logs/2012-08-21/BNL_CVMFS_1-gridgk07"
JobNotification = 3
JobPrio = 0
JobStatus = 1
JobUniverse = 9
KillSig = "SIGTERM"
LastSuspensionTime = 0
LeaveJobInQueue = false
LocalSysCpu = 0.0
LocalUserCpu = 0.0
MATCH_APF_QUEUE = "BNL_CVMFS_1-gridgk07"
MaxHosts = 1
MinHosts = 1
MyType = "Job"
NiceUser = false
Nonessential = true
NotifyUser = "jhover@bnl.gov"
NumCkpts = 0
NumGlobusSubmits = 0
NumJobStarts = 0
NumRestarts = 0
NumSystemHolds = 0
OnExitHold = false
OnExitRemove = true
Out = "/home/apf/factory/logs/2012-08-21/BNL_CVMFS_1-gridgk07//769012.0.out"
Owner = "apf"
PeriodicHold = false
PeriodicRelease = false
PeriodicRemove = false
QDate = 1345558923
Rank = 0.0
RemoteSysCpu = 0.0
RemoteUserCpu = 0.0
RemoteWallClockTime = 0.0
RequestCpus = 1
RequestDisk = DiskUsage
RequestMemory = ifthenelse(MemoryUsage =!= undefined,MemoryUsage,( ImageSize + 1023 ) / 1024)
Requirements = true
RootDir = "/"
ShouldTransferFiles = "YES"
StreamErr = false
StreamOut = false
TargetType = "Machine"
TotalSuspensions = 0
TransferIn = false
UserLog = "/home/apf/factory/logs/2012-08-21/BNL_CVMFS_1-gridgk07/769012.0.log"
WantCheckpoint = false
WantClaiming = false
WantRemoteIO = true
WantRemoteSyscalls = false
WhenToTransferOutput = "ON_EXIT_OR_EVICT"
x509UserProxyEmail = "jhover@bnl.gov"
x509UserProxyExpiration = 1346126473
x509UserProxyFirstFQAN = "/atlas/usatlas/Role=production/Capability=NULL"
x509UserProxyFQAN = "/DC=org/DC=doegrids/OU=People/CN=John R. Hover 47116,/atlas/usatlas/Role=production/Capability=NULL,/atlas/lcg1/Role=NULL/Capability=NULL,/atlas/usatlas/Role=NULL/Capability=NULL,/atlas/Role=NULL/Capability=NULL"
x509userproxysubject = "/DC=org/DC=doegrids/OU=People/CN=John R. Hover 47116"
x509userproxy = "/tmp/prodProxy"
x509UserProxyVOName = "atlas"
    """

    log = logging.getLogger('autopyfactory')
    now = datetime.datetime.utcnow()
    joblist = []
    lines = output.split('\n')
    for line in lines:
        jobidline = None
        if line.strip().startswith('**'):
            jobidline = line.split()
            procid = jobidline[2]
            procid = procid.replace(':','') # remove trailing colon
            ji = JobInfo(procid, 'submitted', now)
            joblist.append(ji)
    if not len(joblist) > 0:
        log.debug('joblist has length 0, returning None')
        joblist = None

    log.debug('Leaving with joblist = %s' %joblist )
    return joblist


def mincondorversion(major, minor, release):
    """
    Call which sets a minimum HTCondor version. If the existing version is too low, it throws an exception.
    
    """

    log = logging.getLogger('autopyfactory')
    s,o = commands.getstatusoutput('condor_version')
    if s == 0:
        cvstr = o.split()[1]
        log.debug('Condor version is: %s' % cvstr)
        maj, min, rel = cvstr.split('.')
        maj = int(maj)
        min = int(min)
        rel = int(rel)
        
        if maj < major:
            raise CondorVersionFailure("HTCondor version %s too low for the CondorEC2BatchSubmitPlugin. Requires 8.1.2 or above." % cvstr)
        if maj == major and min < minor:
            raise CondorVersionFailure("HTCondor version %s too low for the CondorEC2BatchSubmitPlugin. Requires 8.1.2 or above." % cvstr)
        if maj == major and min == minor and rel < release:
            raise CondorVersionFailure("HTCondor version %s too low for the CondorEC2BatchSubmitPlugin. Requires 8.1.2 or above." % cvstr)
    else:
        ec2log.error('condor_version program not available!')
        raise CondorVersionFailure("HTCondor required but not present!")


def checkCondor():
    """
    Perform sanity check on condor environment.
    Does condor_q exist?
    Is Condor running?
    """
    
    # print condor version
    log = logging.getLogger('autopyfactory')
    (s,o) = commands.getstatusoutput('condor_version')
    if s == 0:
        log.debug('Condor version is: \n%s' % o )       
        CONDOR_CONFIG = os.environ.get('CONDOR_CONFIG', None)
        if CONDOR_CONFIG:
            log.debug('Environment variable CONDOR_CONFIG set to %s' %CONDOR_CONFIG)
        else:
            log.debug("Condor config is: \n%s" % commands.getoutput('condor_config_val -config'))
    else:
        log.error('checkCondor() has been called, but not Condor is available on system.')
        raise ConfigFailure("No Condor available on system.")


def statuscondor(queryargs = None):
    """
    Return info about job startd slots. 
    """
    log = logging.getLogger('autopyfactory')
    cmd = 'condor_status -xml '
    if queryargs:
        cmd += queryargs
    log.debug('Querying cmd = %s' %cmd.replace('\n','\\n'))
    before = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.debug('%s seconds to perform the query' %delta)
    if p.returncode == 0:
        log.debug('Leaving with OK return code.')
    else:
        log.warning('Leaving with bad return code. rc=%s err=%s out=%s' %(p.returncode, err, out ))
        out = None
    return out


def statuscondormaster(queryargs = None):
    """
    Return info about masters. 
    """
    log = logging.getLogger('autopyfactory')
    cmd = 'condor_status -master -xml '
    if queryargs:
        cmd += queryargs
    
    log.debug('Querying cmd = %s' % cmd.replace('\n','\\n'))
    #log.debug('Querying cmd = %s' % cmd)
    before = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.debug('It took %s seconds to perform the query' %delta)

    if p.returncode == 0:
        log.debug('Leaving with OK return code.')
    else:
        log.warning('Leaving with bad return code. rc=%s err=%s out=%s' %(p.returncode, err, out ))
        out = None
    return out


def querycondor(queryargs=None, queueskey="match_apf_queue"):
    """
    Query condor for specific job info and return xml representation string
    for further processing.

    queryargs are potential extra query arguments from queues.conf    

    queueskey is the classad being used to distinguish between types of jobs.
    By default it is MATCH_APF_QUEUE
    """

    log = logging.getLogger('autopyfactory')
    log.debug('Starting.')
    queueskey = queueskey.lower()
    querycmd = "condor_q -xml "
    querycmd += " -attributes %s,jobstatus,procid,clusterid " %queueskey
    log.debug('_querycondor: using executable condor_q in PATH=%s' %utils.which('condor_q'))

    # adding extra query args from queues.conf
    if queryargs:
        querycmd += queryargs 

    log.debug('Querying cmd = %s' %querycmd.replace('\n','\\n'))

    before = time.time()          
    p = subprocess.Popen(querycmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)     
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.debug('condor_q: %s seconds to perform the query' %delta)

    if p.returncode == 0:
        log.debug('Leaving with OK return code.')
    else:
        # lets try again. Sometimes RC!=0 does not mean the output was bad
        if out.startswith('<?xml version="1.0"?>'):
            log.warning('RC was %s but output is still valid' %p.returncode)
        else:
            log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
            out = None
    log.debug('_querycondor: Out is %s' % out)
    log.debug('_querycondor: Leaving.')
    return out
    


def querycondorxml(queryargs=None):
    """
    Return human readable info about startds. 
    """
    log = logging.getLogger('autopyfactory')
    cmd = 'condor_q -xml '

    # adding extra query args from queues.conf
    if queryargs:
        querycmd += queryargs 
       
    log.debug('Querying cmd = %s' %cmd.replace('\n','\\n'))
    before = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.debug('It took %s seconds to perform the query' %delta)
    if p.returncode == 0:
        log.debug('Leaving with OK return code.')
    else:
        log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
        out = None
    log.debug('Out is %s' % out)
    log.debug('Leaving.')
    return out


def _xml2nodelist(input):
    log = logging.getLogger('autopyfactory')
    xmldoc = xml.dom.minidom.parseString(input).documentElement
    nodelist = []
    for c in _listnodesfromxml(xmldoc, 'c') :
        node_dict = _node2dict(c)
        nodelist.append(node_dict)
    log.debug('_parseoutput: Leaving and returning list of %d entries.' %len(nodelist))
    log.debug('Got list of %d entries.' %len(nodelist))
    return nodelist


def parseoutput(output):
    """
    parses XML output of condor_q command with an arbitrary number of attribute -format arguments,
    and creates a Python List of Dictionaries of them. 
    
    Input:
    <!DOCTYPE classads SYSTEM "classads.dtd">
    <classads>
        <c>
            <a n="match_apf_queue"><s>BNL_ATLAS_1</s></a>
            <a n="jobstatus"><i>2</i></a>
        </c>
        <c>
            <a n="match_apf_queue"><s>BNL_ATLAS_1</s></a>
            <a n="jobstatus"><i>1</i></a>
        </c>
    </classads>                       
    
    Output:
    [ { 'match_apf_queue' : 'BNL_ATLAS_1',
        'jobstatus' : '2' },
      { 'match_apf_queue' : 'BNL_ATLAS_1',
        'jobstatus' : '1' }
    ]
    
    If the query has no 'c' elements, returns empty list
    
    """

    log = logging.getLogger('autopyfactory')
    log.debug('Starting.')                

    # first convert the XML output into a list of XML docs
    outputs = _out2list(output)

    nodelist = []
    for output in outputs:
        xmldoc = xml.dom.minidom.parseString(output).documentElement
        for c in _listnodesfromxml(xmldoc, 'c') :
            node_dict = _node2dict(c)
            nodelist.append(node_dict)            
    log.debug('Got list of %d entries.' %len(nodelist))       
    return nodelist


def _out2list(xmldoc):
    """
    converts the xml output of condor_q into a list.
    This is in case the output is a multiple XML doc, 
    as it happens when condor_q -g 
    So each part of the output is one element of the list
    """

    # we assume the header of each part of the output starts
    # with string '<?xml version="1.0"?>'
    #indexes = [m.start() for m in re.finditer('<\?xml version="1.0"\?>',  xmldoc )]
    indexes = [m.start() for m in re.finditer('<\?xml',  xmldoc )]
    if len(indexes)==1:
        outs = [xmldoc]
    else:
        outs = []
        for i in range(len(indexes)):
            if i == len(indexes)-1:
                tmp = xmldoc[indexes[i]:]
            else:
                tmp = xmldoc[indexes[i]:indexes[i+1]]
            outs.append(tmp)
    return outs



def _listnodesfromxml( xmldoc, tag):
    return xmldoc.getElementsByTagName(tag)


def _node2dict(node):
    """
    parses a node in an xml doc, as it is generated by 
    xml.dom.minidom.parseString(xml).documentElement
    and returns a dictionary with the relevant info. 
    An example of output looks like
           {'globusstatus':'32', 
             'match_apf_queue':'UC_ITB', 
             'jobstatus':'1'
           }        
    
    
    """
    log = logging.getLogger('autopyfactory')
    dic = {}
    for child in node.childNodes:
        if child.nodeType == child.ELEMENT_NODE:
            key = child.attributes['n'].value
            #log.debug("child 'n' key is %s" % key)
            if len(child.childNodes[0].childNodes) > 0:
                try:
                    value = child.childNodes[0].firstChild.data
                    dic[key.lower()] = str(value)
                except AttributeError:
                    dic[key.lower()] = 'NONE'
    return dic


def aggregateinfo(input, queueskey="match_apf_queue"):
    """
    This function takes a list of job status dicts, and aggregates them by queue,
    ignoring entries without MATCH_APF_QUEUE
    
    Assumptions:
      -- Input has a single level of nesting, and consists of dictionaries.
      -- You are only interested in the *count* of the various attributes and value 
      combinations. 
     
    Example input:
    [ { 'match_apf_queue' : 'BNL_ATLAS_1',
        'jobstatus' : '2' },
      { 'match_apf_queue' : 'BNL_ATLAS_1',
        'jobstatus' : '1' }
    ]                        
    
    Output:
    { 'UC_ITB' : { 'jobstatus' : { '1': '17',
                                   '2' : '24',
                                   '3' : '17',
                                 },
                   'globusstatus' : { '1':'13',
                                      '2' : '26',
                                      }
                  },
    { 'BNL_TEST_1' :{ 'jobstatus' : { '1':  '7',
                                      '2' : '4',
                                      '3' : '6',
                                 },
                   'globusstatus' : { '1':'12',
                                      '2' : '46',
                                      }
                  }, 
                  
    If input is empty list, output is empty dictionary
                 
    """
    log = logging.getLogger('autopyfactory')
    log.debug('Starting with list of %d items.' % len(input))
    queues = {}
    for item in input:
        if not item.has_key(queueskey):
            # This job is not managed by APF. Ignore...
            continue
        apfqname = item[queueskey]
        # get current dict for this apf queue
        try:
            qdict = queues[apfqname]
        # Or create an empty one and insert it.
        except KeyError:
            qdict = {}
            queues[apfqname] = qdict    
        
        # Iterate over attributes and increment counts...
        for attrkey in item.keys():
            # ignore the queueskey attrbute. 
            if attrkey == queueskey:
                continue
            attrval = item[attrkey]
            # So attrkey : attrval in joblist
            # Get current attrdict for this attribute from qdict
            try:
                attrdict = qdict[attrkey]
            except KeyError:
                attrdict = {}
                qdict[attrkey] = attrdict
            
            try:
                curcount = qdict[attrkey][attrval]
                qdict[attrkey][attrval] = curcount + 1                    
            except KeyError:
                qdict[attrkey][attrval] = 1
                   
    log.debug('Aggregate: output is %s ' % queues) 
    log.debug('Aggregate: Created dict with %d queues.' % len(queues))
    return queues

  

def getJobInfo():
    log = logging.getLogger('autopyfactory')
    xml = querycondorxml()
    nl = _xml2nodelist(xml)
    log.debug("Got node list of length %d" % len(nl))
    joblist = []
    qd = {}
    if len(nl) > 0:
        for n in nl:
            j = CondorEC2JobInfo(n)
            joblist.append(j)
        
        indexhash = {}
        for j in joblist:
            try:
                i = j.match_apf_queue
                indexhash[i] = 1
            except:
                # We don't care about jobs not from APF
                pass

        for k in indexhash.keys():
        # Make a list for jobs for each apfqueue
            qd[k] = []
        
        # We can now safely do this..
        for j in joblist:
            try:
                index = j.match_apf_queue
                qjl = qd[index]
                qjl.append(j)
            except:
                # again we don't care about non-APF jobs
                pass    
            
    log.debug("Made job list of length %d" % len(joblist))
    log.debug("Made a job info dict of length %d" % len(qd))
    return qd


def getStartdInfoByEC2Id():
    log = logging.getLogger('autopyfactory')
    out = statuscondor()
    nl = _xml2nodelist(out)
    infolist = {}
    for n in nl:
        #print(n)
        try:
            ec2iid = n['ec2instanceid']
            state = n['state']
            act = n['activity']
            slots = n['totalslots']
            machine = n['machine']
            j = CondorStartdInfo(ec2iid, machine, state, act)
            #log.debug("Created csdi: %s" % j)
            j.slots = slots
            infolist[ec2iid] = j
        except Exception, e:
            log.error("Bad node. Error: %s" % str(e))
    return infolist
    

def killids(idlist):
    """
    Remove all jobs by jobid in idlist.
    Idlist is assumed to be a list of complete ids (<clusterid>.<procid>)
     
    """
    log = logging.getLogger('autopyfactory')
    idstring = ' '.join(idlist)
    cmd = 'condor_rm %s' % idstring
    log.debug('Issuing remove cmd = %s' %cmd.replace('\n','\\n'))
    before = time.time()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out = None
    (out, err) = p.communicate()
    delta = time.time() - before
    log.debug('It took %s seconds to perform the command' %delta)
    if p.returncode == 0:
        log.debug('Leaving with OK return code.')
    else:
        log.warning('Leaving with bad return code. rc=%s err=%s' %(p.returncode, err ))
        out = None
    

class CondorRequest(object):
    """
    class to define any arbitrary condor task 
    (condor_submit, condor_on, condor_off...)

    The instances of this class can be piped into a Queue() object
    for serialization
    """

    def __init__(self):

        self.cmd = None
        self.args = None
        self.out = None
        self.err = None
        self.rc = None
        self.precmd = None
        self.postcmd = None




class _processcondorrequests(_thread):
    """
    class to process objects
    of class CondorRequest()
    """

    def __init__(self, factory):

        _thread.__init__(self)
        factory.threadsregistry.add("util", self)
        
        self.factory = factory


    def _run(self):
        if not condorrequestsqueue.empty():
            req = condorrequestsqueue.get() 
            if req.cmd == 'condor_submit':       
                self.submit(req)    


    def submit(self, req):
        """
        req is an object of class CondorRequest()
        """
    
        cmd = req.cmd
        if req.args:
            cmd += req.args
        
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
        (out, err) = p.communicate()
        rc = p.returncode
    
        req.out = out
        req.err = err
        req.rc = rc


# The Singleton implementation
class ProcessCondorRequests(object):

    instance = None
    
    def __new__(cls, *k, **kw):
        if not ProcessCondorRequests.instance:
            ProcessCondorRequests.instance = _processcondorrequests(*k, **kw)
        return ProcessCondorRequests.instance




#############################################################################
#               using HTCondor python bindings
#############################################################################

import htcondor
import classad
import copy


def condorhistorylib():

    schedd = htcondor.Schedd()
    history = schedd.history('True', ['MATCH_APF_QUEUE', 'JobStatus', 'EnteredCurrentStatus', 'RemoteWallClockTime'], 0)
    return history


def filtercondorhistorylib(history, constraints=[]):

    # contraints example ['JobStatus == 4', 'RemoteWallClockTime < 120']

    out = []
    for job in history:
        if _matches_constraints(job, constraints):
            out.append(job)
    return out


    

def querycondorlib(remotecollector=None, remoteschedd=None, extra_attributes=[], queueskey='match_apf_queue'):
    """ 
    queries condor to get a list of ClassAds objects
    We query for a few specific ClassAd attributes
    (faster than getting everything)

    remotecollector and remoteschedd
    are passed when querying a remote HTCondor pool 
    They are the equivalent to -pool and -name input
    options to CLI condor_q
    
    extra_attributes are classads needed other than 'jobstatus'
    """

    log = logging.getLogger('autopyfactory')

    if remotecollector:
        # FIXME: to be tested
        log.debug("querying remote pool %s" %remotecollector)
        collector = htcondor.collector(remotecollector)
        scheddAd = collector.locate(condor.DaemonTypes.Schedd, remoteschedd)
        schedd = htcondor.Schedd(scheddAd) 
    else:
        schedd = htcondor.Schedd() # Defaults to the local schedd.

    list_attrs = [queueskey, 'jobstatus']
    list_attrs += extra_attributes
    out = schedd.query('true', list_attrs)
    out = _aggregateinfolib(out, 'jobstatus', queueskey) 
    log.debug(out)
    return out 


def _aggregateinfolib(input, key=None, queueskey='match_apf_queue'):
    # input is a list of job classads
    # key can be, for example: 'jobstatus'    
    # output is a dict[apfqname] [key] [value] = # of jobs with that value

    log = logging.getLogger('autopyfactory')

    queues = {}
    for job in input:
        if not queueskey in job.keys():
            # This job is not managed by APF. Ignore...
            continue
        apfqname = job[queueskey]
        if apfqname not in queues.keys():
            queues[apfqname] = {}
            queues[apfqname][key] = {}

        value = str(job[key])
        if value not in queues[apfqname][key].keys():
            queues[apfqname][key][value] = 0
        queues[apfqname][key][value] += 1
    
    log.debug(queues)
    return queues


def querystatuslib():
    """ 
    Equivalent to condor_status
    We query for a few specific ClassAd attributes 
    (faster than getting everything)
    Output of collector.query(htcondor.AdTypes.Startd) looks like

     [
      [ Name = "slot1@mysite.net"; Activity = "Idle"; MyType = "Machine"; TargetType = "Job"; State = "Unclaimed"; CurrentTime = time() ], 
      [ Name = "slot2@mysite.net"; Activity = "Idle"; MyType = "Machine"; TargetType = "Job"; State = "Unclaimed"; CurrentTime = time() ]
     ]
    """
    # We only want to try to import if we are actually using the call...
    # Later on we will need to handle Condor version >7.9.4 and <7.9.4
    #

    collector = htcondor.Collector()
    list_attrs = ['Name', 'State', 'Activity']
    outlist = collector.query(htcondor.AdTypes.Startd, 'true', list_attrs)
    return outlist



def _matches_constraints(ad, constraints):
    constraint_expression = " && ".join( ["TARGET." + i for i in constraints])
    return _matches_constraint_expr(ad, constraint_expression)


def _matches_contraint_expr(ad, constraint_expression):
    req_ad = classad.ClassAd()
    req_ad['Requirements'] = classad.ExprTree(constraint_expression)
    return ad.matches(req_ad)



##############################################################################

def test1():
    infodict = getJobInfo()
    ec2jobs = infodict['BNL_CLOUD-ec2-spot']    
    #pprint(ec2jobs)
    
    startds = getStartdInfoByEC2Id()    
    print(startds)
