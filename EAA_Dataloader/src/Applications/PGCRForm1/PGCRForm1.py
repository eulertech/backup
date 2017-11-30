'''
Created on Jan 25, 2017
'''
import os
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase
from .Form1 import Form1

class PGCRForm1(ApplicationBase):
    '''
    Handles Form1 files in the PGCR project
    '''
    def __init__(self):
        '''
        Constructor for this class
        '''
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
        self.awsParams = ""

    def DownloadFile(self, s3Key, localGzipFilepath):
        '''
        Wrapper to download file
        '''
        self.logger.info(self.moduleName + " Downloading file: " + s3Key)
        try:
            S3Utilities.DownloadFileFromS3(self.awsParams.s3, self.job["bucketName"], s3Key, localGzipFilepath)
        except Exception as e:
            self.logger.exception("Error while downloading file: {}".format(s3Key))
            self.logger.exception("{}".format(str(e)))
            raise

    def GetFileFromS3(self):
        '''
        Downloads the file from the S3 bucket to the data folder
        '''
        for srcFileParameter in self.job["foxpro_files"]:
            s3Key = str(self.job["s3SrcDirectory"]) + srcFileParameter["Name"]
            localGzipFilepath = self.localTempDirectory + "/" + srcFileParameter["Name"]
            #download the DBF file
            self.DownloadFile(s3Key + ".DBF", localGzipFilepath + ".DBF")
            if srcFileParameter.get("GetFPTFile") is not None: #download the FPT file if the key "GetFPTFile" is present in the config
                self.DownloadFile(s3Key + ".FPT", localGzipFilepath + ".FPT")

    def Start(self, logger, moduleName, filelocs):
        '''
        Starting point of this Project
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)            
            self.GetFileFromS3()
            Form1(logger, self.fileUtilities, self.localTempDirectory, self.job, self.awsParams) #invoke the Form1 handler
        except:
            self.logger.exception(moduleName + " - Exception!")
            raise

