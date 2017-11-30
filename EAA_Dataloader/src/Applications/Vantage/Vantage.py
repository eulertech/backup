'''
Created on Jul 4, 2017

@author: Hector Hernandez
@summary: Loads the OPEC Calendar data excel file pulled from the OPEC web site.

'''

import os

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase

class Vantage(ApplicationBase):
    '''
    This class is used to get the Vanatage data from IHS Vantage Database, transform it and load it into Redshift.
    '''

    def __init__(self):
        '''
        Initial settings
        '''
        super(Vantage, self).__init__()

        self.awsParams = ""
        self.packedFolder = None
        self.rawFolder = None
        self.fileUtilities = FileUtilities(self.logger)
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def BulkExtractAll(self):
        '''
        Controls the flow thru the different data sets coming from Vantage DB.
        '''
        try:
            for dsScript in self.job["extractingScripts"]:
                self.logger.info(self.moduleName + " Starts extracting " + dsScript["tableSuffix"] + " data...")

                self.bcpUtilities.RunBCPJob(self.job["mssqlLoginInfo"],
                                            self.job["bcpUtilityDirOnLinux"],
                                            self.fileUtilities.LoadSQLQuery(self.location + dsScript["scriptFile"]),
                                            self.localTempDirectory + "/Raw/" + dsScript["tableSuffix"] + ".CSV",
                                            self.job["delimiter"])
        except Exception as err:
            self.logger.error("Error while trying to Bulk Extract all. Message: " + err.message)
            raise

    def TransformAndPackAll(self):
        '''
        Compress the csv files created.
        '''
        rawFiles = self.fileUtilities.ScanFolder(self.rawFolder, None, "CSV")

        try:
            for rFile in rawFiles:
                rFileFull = self.rawFolder + "/" + rFile

                self.logger.info(self.moduleName + " started compressing file: " + rFile)

                self.fileUtilities.GzipFile(rFileFull,
                                            self.packedFolder + "/" + rFile + ".GZ")

                self.fileUtilities.RemoveFileIfItExists(rFileFull)
        except Exception as err:
            self.logger.error(self.moduleName + " Error while compressing raw files. Message: " + err.message)
            raise

    def LoadAllFromS3(self):
        '''
        Load all CSVs from the Vantage's S3 bucket into Redshift
        '''
        rsConnect = None

        try:
            s3DataFolder = "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"]

            rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                                  host=self.awsParams.redshift['Hostname'],
                                                  port=self.awsParams.redshift['Port'],
                                                  user=self.awsParams.redshiftCredential['Username'],
                                                  password=self.awsParams.redshiftCredential['Password'])

            for dsScript in self.job["extractingScripts"]:
                RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                                 {
                                                     "destinationSchema": self.job["destinationSchema"],
                                                     "tableName": self.job["tableName"] + dsScript["tableSuffix"],
                                                     "s3Filename": s3DataFolder + "/" + dsScript["tableSuffix"] + ".CSV.GZ",
                                                     "fileFormat": self.job["fileFormat"],
                                                     "dateFormat": self.job["dateFormat"],
                                                     "delimiter": self.job["delimiter"]
                                                 },
                                                 self.logger, "N")

            self.logger.info(self.moduleName + " - Cleaning s3 data folder...")

            S3Utilities.DeleteFileFromS3TempUsingAWSCLi(s3DataFolder, "--recursive --quiet")
        except Exception:
            self.logger.error(self.moduleName + " - Error while trying to save into Redshift from s3 folder.")
            raise
        finally:
            if rsConnect is not None:
                rsConnect.close()

    def BulkUploadToS3(self):
        '''
        Uploads all GZIP files created into S3 to be uploaded later...
        '''
        self.logger.info(self.moduleName + " - Uploading GZIP files to s3 folder...")

        S3Utilities.CopyItemsAWSCli(self.packedFolder,
                                    "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"],
                                    "--recursive --quiet")

    def Start(self, logger, moduleName, filelocs):
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)

            self.packedFolder = self.localTempDirectory + "/Packed"
            self.rawFolder = self.localTempDirectory + "/Raw"

            self.fileUtilities.RemoveFolder(self.packedFolder)
            self.fileUtilities.RemoveFolder(self.rawFolder)

            self.fileUtilities.CreateFolder(self.packedFolder)
            self.fileUtilities.CreateFolder(self.rawFolder)

            self.BulkExtractAll()
            self.TransformAndPackAll()
            self.BulkUploadToS3()
            self.LoadAllFromS3()
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            raise Exception(err.message)
