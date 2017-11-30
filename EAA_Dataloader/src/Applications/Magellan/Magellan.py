'''
Created on May 18, 2017

@author: Thomas Coffey
@summary: this is the primary driver to load all Magellan data
    process: if pass a value of '1'
    1) make sure we have a clean local area
    2) create sql scripts
    3) create tables
    4) pull all zip files to local area

    process: if pass a value of '2'
    1)  call the process to create csvs

    process: if pass a value of '3'
    1)  gzip the csv files
    2)  push all gzip files to S3
    3)  create manifest file for data and attribute files
    4)  load the manifest data into RedShift attribute and data

    process: if pass a value of '4'
    1)  clean up local folders
    2)  clean up S3 temp folder
'''

import sys
import os

from GetParameters import GetParameters as GetMainParameters
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.RedshiftUtilities import RedshiftUtilities

from Applications.Common.ApplicationBase import ApplicationBase
from Applications.Magellan.MagellanUtilities import MagellanUtilities
# pylint: disable=too-many-instance-attributes
class MagellanProcess(ApplicationBase):
    '''
    This application will pull Magellan data from the MarkLogic system AKA ODATA
    '''

    def __init__(self):
        '''
        Constructor
        '''
        super(MagellanProcess, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
        self.processParams = GetMainParameters()
        self.sqlFolder = None
        self.csvFolder = None
        self.zipFolder = None
        self.gzipFolder = None
        self.moduleName = 'Magellan'

    def PullData(self):
        '''
        routine to pull data from s3 to local instance
        '''
        sourceLocation = "s3://" + self.job["bucketName"] + self.job["s3DataFolder"]
        try:
            destLocation = self.localTempDataDirectory
            S3Utilities.CopyItemsAWSCli(sourceLocation,
                                        destLocation,
                                        '''--recursive --quiet --include "*.zip"''')
        except:
            self.logger.exception(self.moduleName + " had an issue in pullData for " + sourceLocation)
            raise

    def CreateFolder(self, folder, createIt):
        '''
        if a folder needs to be created it does it and if we want a fresh folder it will do that to
        '''
        try:
            fName = folder["name"]
            tfoldername = self.localTempDirectory + "/" + folder["folder"] + "/"
            if fName == "sql":
                self.sqlFolder = tfoldername
            elif fName == "csv":
                self.csvFolder = tfoldername
            elif fName == "zips":
                self.zipFolder = tfoldername
            elif fName == "gzips":
                self.gzipFolder = tfoldername
            if createIt == "Y":
                if folder["new"] == "Y":
                    FileUtilities.RemoveFolder(tfoldername)
                FileUtilities.CreateFolder(tfoldername)
        except:
            self.logger.exception(self.moduleName + " had an issue in CreateFolder for " + folder)
            raise

    def CreateFolders(self, createIt):
        '''
        create all the local folders we will need
        '''
        try:
            for fld in self.processParams.configdata["folders"]:
                self.CreateFolder(fld, createIt)
        except:
            self.logger.exception(self.moduleName + "- we had an error in CreateFolders")
            raise

    def SetupEnvironment(self):
        '''        using this parameter value will perform these steps
        1)  create working folders for csv, sql, zips
        2)  pull all the zip files down from s#
        '''
        self.logger.info("Starting SetupEnvironment signals the start of the process")
        self.CreateFolders("Y")
        muHelper = MagellanUtilities()
        muHelper.logger = self.logger
        commonParams = {}
        commonParams["sqlFolder"] = self.sqlFolder
        muHelper.commonParams = commonParams
        muHelper.CreateSQLFiles(self.job, self.job["destinationSchema"])
        muHelper.BuildTables(self.job["tables"])

    def ProcessJsonFiles(self, fBatch):
        '''
        load up some variables before we call the process to create csv from json files
        '''
        self.CreateFolders("N")  #  this just sets the variable we will need
        commonParams = {}
        commonParams["moduleName"] = "Magellan"
        commonParams["zipFolder"] = self.zipFolder
        commonParams["localTempDirectory"] = self.localTempDataDirectory
        commonParams["loggerParams"] = "log"
        commonParams["attrFields"] = self.job["tables"][0]["fields"]
        commonParams["csvFolder"] = self.csvFolder
        commonParams["gzipFolder"] = self.gzipFolder
        mu = MagellanUtilities()
        mu.commonParams = commonParams
        mu.fl = fBatch
        mu.StartHere()

###
#  we are not doing this for now
###
    def LoadAllData(self):
        '''
        Process:
        1)  push Attribute and data gz files to S3
        2)  load data into Redshift from S3
        '''
        self.CreateFolders("N")  #  this just sets the variable we will need
        self.fileUtilities = FileUtilities(self.logger)

        rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                              host=self.awsParams.redshift['Hostname'],
                                              port=self.awsParams.redshift['Port'],
                                              user=self.awsParams.redshiftCredential['Username'],
                                              password=self.awsParams.redshiftCredential['Password'])

        for table in self.job["tables"]:
            ###
            #  first create zip files for all we want to send to S3
            ###
            s3folder = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"]
            if table["type"] == "attributes":
                sourceFolder = self.gzipFolder + "attr"
                destFolder = s3folder + "/attribute"
            else:  # data types
                sourceFolder = self.gzipFolder + "data"
                destFolder = s3folder + "/data"

            S3Utilities.CopyItemsAWSCli(sourceFolder,
                                        destFolder,
                                        '''--recursive --quiet --include "*.gz"''')

            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": self.job["destinationSchema"],
                                                 "tableName": table["name"],
                                                 "s3Filename": destFolder,
                                                 "fileFormat": self.job["fileFormat"],
                                                 "dateFormat": self.job["dateFormat"],
                                                 "delimiter": self.job["delimiter"]
                                             },
                                             self.logger, "N")

#            S3Utilities.DeleteFileFromS3TempUsingAWSCLi(destFolder,
#                                                        '''--recursive --quiet --include "*.gz"''')

        rsConnect.close()

    def CleanupArea(self):
        '''
        1)  clean up the local area on app server
        2)  clean up files in the temp folder on S3
        '''
        for fld in self.processParams.configdata["folders"]:
            if fld["name"] == 'sql':
                self.CreateFolder(fld, "N")
            elif fld["name"] == 'gzips':
                pass
            else:
                self.CreateFolder(fld, "Y")

        user = os.environ.get("USER", "")
        if not user:
            user = os.environ.get("USERNAME", "")

        # Load file to S3 at a temporary location
        bucketName = "ihs-temp"
        s3TempKey = "eaa/src/temp/" + user + "/"
        s3FullPath = "s3://" + bucketName + "/" + s3TempKey

        S3Utilities.DeleteFileFromS3TempUsingAWSCLi(s3FullPath,
                                                    '''--recursive --quiet --include "*.zip"''')

    def Main(self, option):
        '''
        Main application that loads the configuration and runs the applications
        '''
        ###
        # if we passed in a parameter for the config file then use it otherwise we will use the
        # ProcessStepsConfig.json file
        ###
        self.processParams.configfile = self.location + '/jobConfig.json'

        self.processParams.LoadConfigFile()

        ###
        #  Construct the log and output directory relative to the application directory.  This way the
        #  application works on Windows and Linux without change
        ###
    ###
    #  because we are not at the root we are using the config file to identify where to work with files
    ###
        fileLocation = self.processParams.configdata["outputLocations"]["workingFolder"] +\
            self.processParams.configdata["outputLocations"]["locationSuffix"]
        self.processParams.configdata["outputLocations"]["relativeLoggingFolder"] =\
            os.path.join(fileLocation, self.processParams.configdata["outputLocations"]["relativeLoggingFolder"])
        self.processParams.configdata["outputLocations"]["relativeOutputfolder"] =\
            os.path.join(fileLocation, self.processParams.configdata["outputLocations"]["relativeOutputfolder"])
        self.processParams.configdata["outputLocations"]["relativeInputfolder"] =\
            os.path.join(fileLocation, self.processParams.configdata["outputLocations"]["relativeInputfolder"])
        logger = FileUtilities.CreateLogger(self.processParams.configdata["outputLocations"]["relativeLoggingFolder"])
        ApplicationBase.Start(self, logger, self.moduleName, self.processParams.configdata["outputLocations"])
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

        if option == 'PD':
            logger.info("*** Magellan PullData started")
            self.PullData()
            logger.info("*** Magellan PullData completed")
        elif option == 'SE':
            logger.info("*** Magellan SetupEnvironment started")
            self.SetupEnvironment()
            logger.info("*** Magellan SetupEnvironment completed")
        elif option == 'PJ':
#            fBatch = "20170516164925+0200-002111-XML.zip"
            fBatch = sys.argv[2]
            logger.info("*** Magellan ProcessJsonFiles started for " + fBatch)
            self.ProcessJsonFiles(fBatch)
            logger.info("*** Magellan ProcessJsonFiles completed for " + fBatch)
        elif option == 'LD':
            logger.info("*** Magellan LoadAllData started")
            self.LoadAllData()
            logger.info("*** Magellan LoadAllData completed")
        elif option == 'CL':
            logger.info("*** Magellan CleanupArea started")
            self.CleanupArea()
            logger.info("*** Magellan CleanupArea completed")
        elif option == 'EL':
            logger.info("*** Magellan Application Complete.")
        elif option == 'SL':
            logger.info("*** Starting Magellan Application.")
        elif option == 'LP':
            logger.info("NOTE: " + sys.argv[2])
        else:
            logger.error("*** Magellan Application invalid option.")

if __name__ == '__main__':
    mag = MagellanProcess() # pylint: disable=invalid-name
    if len(sys.argv) < 2:
        mag.Main('PJ')
    else:
        mag.Main(sys.argv[1])
