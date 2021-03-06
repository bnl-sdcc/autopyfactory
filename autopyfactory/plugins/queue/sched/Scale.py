#! /usr/bin/env python
#

from autopyfactory.interfaces import SchedInterface
import math
import logging


class Scale(SchedInterface):
    id = 'scale'
    
    """   
    applies an scale factor to the previous value of nsub.
    Returns the input as it is in case there is no scale factor defined.
    """   
 
    def __init__(self, apfqueue, config, section):

        try:
            self.apfqueue = apfqueue                
            self.log = logging.getLogger('autopyfactory.sched.%s' %apfqueue.apfqname)
            self.factor = self.apfqueue.qcl.generic_get(self.apfqueue.apfqname, 'sched.scale.factor', 'getfloat', default_value=1.0)
            self.factor = float(self.factor)
            self.log.debug("SchedPlugin: Object initialized.")
        except Exception as ex:
            self.log.error("SchedPlugin object initialization failed. Raising exception")
            raise ex

    def calcSubmitNum(self, n=0):
        self.log.debug('Starting with n=%s' %n)
        out = math.ceil(n * self.factor)
        out = int(out)  #because the output of ceil() is float
        msg = "Scale:in=%s,factor=%s,ret=%s" %(n, self.factor, out )
        self.log.info(msg)
        return (out, msg) 
