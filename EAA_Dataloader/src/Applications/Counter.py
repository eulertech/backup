'''
Created on Jan 20, 2017

@author: VIU53188
@summary: class created to play with multithreading.  This does nothing more than performs a loop to keep it busy
'''
import os
from AACloudTools.FileUtilities import FileUtilities
from Applications.Common.ApplicationBase import ApplicationBase
class Counter(ApplicationBase):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        super(Counter, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def Start(self, logger, moduleName, filelocs):
        '''
        start it
        '''
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        seedVal = 100
        logger.info("Starting count with seed %s in Counter" % (seedVal))
        for i in range(seedVal):
            val = i+seedVal
#            logger.info("v value in Counter %s" % (str(v)))
        logger.info("this is the starting seed %s and the end value was %s in Counter" % (seedVal, val))
