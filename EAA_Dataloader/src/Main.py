'''
Created on Jan 19, 2017

@author: Thomas Coffey
@summary: This is the primary drive for the Data loader utility
        The purpose of this package is to run a full set of loads based on a jSON configuration file
'''

###
# the git repository for this package is
# http://tfs-emea.ihs.com:8080/tfs/emea_ihs_collection/AdvancedAnalytics/_git/EAA_Dataloader
##
import sys
import os
import platform
from threading import Thread
import importlib

##
#  this is where you include any extra application you want to call
##
from GetParameters import GetParameters as GetMainParameters # pylint: disable=relative-import
from AACloudTools.FileUtilities import FileUtilities # pylint: disable=relative-import
from AACloudTools.EtlLoggingUtilities import EtlLoggingUtilities # pylint: disable=relative-import
from Applications.Common.ApplicationBase import ApplicationBase # pylint: disable=relative-import

def ProcessApps(logger, processArray, folderlocs):
    '''
    ProcessApps process all the applications that are turned on.
    '''

    FileUtilities.CreateFolder(folderlocs["relativeOutputfolder"])
##
#  runs holds a list of the processes we are wanting to run
##
    runs = []
###
#  if you have a new process make sure you add it here
###
    try:
        ab = ApplicationBase()
        ev = ab.LoadEnvironmentVariables(logger)
        if "tblEtl" in folderlocs:
            etlUtilities = EtlLoggingUtilities(logger)
            etlUtilities.awsParams = ev.awsParams
            etlUtilities.appschema = folderlocs["tblEtl"]["appschema"]
            etlUtilities.etlSchema = folderlocs["tblEtl"]["schemaName"]
            
        for proc in processArray:
            module = proc["module"]
            baseName = module.rsplit('.', 1)[1]

            logger.info(baseName + " - Starting module.")
            moduleName = importlib.import_module(module)
            className = getattr(moduleName, baseName)()

            #className.Start(logger, baseName, folderlocs) # For single threading
            # For multi-threading
            runs.append(Thread(name=baseName, target=className.Start, args=(logger, baseName, folderlocs)))

        for rn in runs:
            rn.start()

        for rn in runs:
            rn.join()
            if rn.is_alive() is False and "tblEtl" in folderlocs:
                procid = etlUtilities.GetRunID(folderlocs["tblEtl"]["table"], str(rn.name))
                if procid > -1:
                    etlUtilities.CompleteInstance(folderlocs["tblEtl"]["table"], procid, 'C')

    except:
        logger.exception("Exception processing application modules!")
        raise
    logger.info("All threads complete.")

def Main():
    '''
    Main application that loads the configuration and runs the applications
    '''
    ###
    # if we passed in a parameter for the config file then use it otherwise we will use the
    # ProcessStepsConfig.json file
    ###
    processParams = GetMainParameters()
    if len(sys.argv) < 2:
        processParams.configfile = 'ProcessStepsConfig.json'
    else:
        processParams.configfile = sys.argv[1]
    processParams.LoadConfigFile()
    ###
    #  Construct the log and output directory relative to the application directory.  This way the
    # application works on Windows and Linux without change
    ###
    fileLocation = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) + processParams.configdata["outputLocations"]["locationSuffix"]
    processParams.configdata["outputLocations"]["relativeLoggingFolder"] =\
        os.path.join(fileLocation, processParams.configdata["outputLocations"]["relativeLoggingFolder"])
    processParams.configdata["outputLocations"]["relativeOutputfolder"] =\
        os.path.join(fileLocation, processParams.configdata["outputLocations"]["relativeOutputfolder"])

    logger = FileUtilities.CreateLogger(processParams.configdata["outputLocations"]["relativeLoggingFolder"],
                                        processParams.configdata["debuggingLevel"])
    logger.info("*** Starting Main Application.")

    processArrayJson = processParams.configdata["Processes"]
    processArray = []
###
#  this gets a lists of all the process that you want to run
###
    cPlat = platform.system().upper()[:1]

    for proc in processArrayJson:
        if proc["execute"] == "Y":
            if proc["platform"] == 'W':
                if proc["platform"] == cPlat:
                    processArray.append(proc)
            else:
                processArray.append(proc)
##
#  let's get started
##
    ProcessApps(logger, processArray, processParams.configdata["outputLocations"])
    logger.info("*** Main Application Complete.")

if __name__ == '__main__':
    Main()
