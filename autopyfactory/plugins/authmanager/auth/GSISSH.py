"""
 An APF auth plugin to provide GSISSH key info.
 Much simpler than the full X509 plugin since it  

"""
import base64
import logging
import os
import traceback


# Does not need to be a thread because it doesn't need to perform asynch actions.
class GSISSH(object):
    """
        Container for GSISSH proxy info.
        Works with provided filepaths. 
        Work with config-only provided base64-encoded tokens, with files created.  
        Files written to 
            <authbasedir>/<name>/<proxyfile>
        
    """    
    def __init__(self, manager, config, section):
        self.log = logging.getLogger('autopyfactory.auth')
        self.name = section
        self.manager = manager
        self.factory = manager.factory
        self.basedir = os.path.expanduser(config.get(section, 'authbasedir'))
        self.privkey = config.get(section, 'gsissh.privatekey' )
        self.privkeypath = os.path.expanduser(config.get(section, 'gsissh.privatekeyfile' ))
        
        # Handle raw empty values
        if self.privkey.lower() == 'none':
            self.privkey = None
        
        # Handle path empty values    
        if self.privkeypath.lower() == 'none':
            self.privkeypath = None
              
        # Create files if needed
        if self.privkey is not None:
            fdir = "%s/%s" % (self.basedir, self.name)
            fpath = "%s/%s" % (fdir, self.sshtype)
            try:
                self._ensuredir(fdir)
                self._decodewrite(fpath, self.privkey)
                self.privkeypath = fpath
                os.chmod(fpath, 0o600)
                self.log.debug("Wrote decoded private key to %s and set config OK." % self.privkeypath)
            except Exception as e:
                self.log.error("Exception: %s" % str(e))
                self.log.debug("Exception: %s" % traceback.format_exc())
                       
        self.log.debug("GSISSH Handler for profile %s initialized." % self.name)

        
    def _ensuredir(self, dirpath):
        self.log.debug("Ensuring directory %s" % dirpath)
        if not os.path.exists(dirpath):
            os.makedirs(dirpath)
    
    def _decodewrite(self, filepath, b64string ):
        self.log.debug("Writing key to %s" % filepath)
        decoded = GSISSH.decode(b64string)
        try:
            fh = open(filepath, 'w')
            fh.write(decoded)
            fh.close()
        except Exception as e:
            self.log.error("Exception: %s" % str(e))
            self.log.debug("Exception: %s" % traceback.format_exc())
            raise
        else:
            fh.close()
            
            
    def _validate(self):
        """
        Confirm credentials exist and are valid. 
        """
        return True

    
    def getGSISSHPrivKey(self):
        pass
    
   
    def getGSISSHPrivKeyFilePath(self):
        self.log.debug('[%s] Retrieving privkeypath: %s' % (self.name, self.privkeypath))
        return self.privkeypath
    
##############################################
#        External Utility class methods. 
##############################################

    @classmethod
    def encode(self, string):
        return base64.b64encode(string)
    
    @classmethod
    def decode(self, string):
        return base64.b64decode(string)    
        
