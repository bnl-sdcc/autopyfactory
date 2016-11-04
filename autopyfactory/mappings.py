#!/usr/bin/env python
'''
classes and methods to map structures of info
from one type to another
'''
import logging
from Queue import Queue

import autopyfactory.utils as utils
from autopyfactory.apfexceptions import ConfigFailure, CondorVersionFailure
from autopyfactory.info import JobInfo


log = logging.getLogger('main.mappings')



def map2info(input, info_object):
    '''
    This takes aggregated info by queue, with condor/condor-g specific status totals, and maps them 
    to the backend-agnostic APF BatchStatusInfo object.
    
       APF             Condor-C/Local              Condor-G/Globus 
    .pending           Unexp + Idle                PENDING
    .running           Running                     RUNNING
    .suspended         Held                        SUSPENDED
    .done              Completed                   DONE
    .unknown           
    .error
    
    Primary attributes. Each job is in one and only one state:
        pending            job is queued (somewhere) but not running yet.
        running            job is currently active (run + stagein + stageout)
        error              job has been reported to be in an error state
        suspended          job is active, but held or suspended
        done               job has completed
        unknown            unknown or transient intermediate state
        
    Secondary attributes. Each job may be in more than one category. 
        transferring       stagein + stageout
        stagein
        stageout           
        failed             (done - success)
        success            (done - failed)
        ?
    
      The JobStatus code indicates the current status of the job.
        
                Value   Status
                0       Unexpanded (the job has never run)
                1       Idle
                2       Running
                3       Removed
                4       Completed
                5       Held
                6       Transferring Output

        The GlobusStatus code is defined by the Globus GRAM protocol. Here are their meanings:
        
                Value   Status
                1       PENDING 
                2       ACTIVE 
                4       FAILED 
                8       DONE 
                16      SUSPENDED 
                32      UNSUBMITTED 
                64      STAGE_IN 
                128     STAGE_OUT 
    Input:
      Dictionary of APF queues consisting of dicts of job attributes and counts.
      { 'UC_ITB' : { 'Jobstatus' : { '1': '17',
                                   '2' : '24',
                                   '3' : '17',
                                 },
                  }
       }          
    Output:
        A BatchStatusInfo object which maps attribute counts to generic APF
        queue attribute counts. 
    '''

    log.debug('Starting.')

    try:
        for site in input.keys():
            qi = queue_info_class()
            qi = info_object.default()
            info_object[site] = qi
            attrdict = input[site]
            valdict = attrdict['jobstatus']
            qi.fill(valdict, mappings=self.jobstatus2info)

    except Exception, ex:
        self.log.error("Exception: %s" % str(e))
        self.log.error("Exception: %s" % traceback.format_exc())
                    
    info_object.lasttime = int(time.time())
    log.debug('Returning %s: %s' % (info_object.__class__.__name__, info_object))
    for site in info_object.keys():
        log.debug('Queue %s = %s' % (site, info_object[site]))           

    return info_object 

