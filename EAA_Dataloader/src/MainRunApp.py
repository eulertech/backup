'''
Created on Oct 10, 2017

@author: Christopher lewis
@summary: 
        Derived from Main.py
        MainRun1 will be called from Airflow
        Run only the specified application
        This is the primary drive for the Data loader utility
'''

###
# the git repository for this package is
# http://tfs-emea.ihs.com:8080/tfs/emea_ihs_collection/AdvancedAnalytics/_git/EAA_Dataloader
##
import sys
import os
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
            className.Start(logger, baseName, folderlocs) # For single threading

            if "tblEtl" in folderlocs:
                procid = etlUtilities.GetRunID(folderlocs["tblEtl"]["table"], str(baseName))
                if procid > -1:
                    etlUtilities.CompleteInstance(folderlocs["tblEtl"]["table"], procid, 'C')

    except:
        logger.exception("Exception processing application modules!")
        raise
    logger.info(baseName + " - module COMPLETED.")

def Main():
    '''
    Main application that loads the configuration and runs the applications
    '''
    ###
    # if we passed in a parameter for the config file then use it otherwise we will use the
    # ProcessStepsConfig.json file
    ###
    processParams = GetMainParameters()
    processParams.configfile = 'ProcessStepsConfig.json'
    if len(sys.argv) < 2:
        appToRun = "Applications.Counter"
    else:
        appToRun = sys.argv[1]
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
    for proc in processArrayJson:
        if proc["module"] == appToRun:
                processArray.append(proc)
##
#  let's get started
##
    ProcessApps(logger, processArray, processParams.configdata["outputLocations"])
    logger.info("*** Main Application Complete.")

if __name__ == '__main__':
    Main()
