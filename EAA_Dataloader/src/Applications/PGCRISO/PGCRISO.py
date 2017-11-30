'''
Created on Jan 25, 2017
'''
import os
from Applications.Common.ApplicationBase import ApplicationBase
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from ISO import ISO #pylint: disable=relative-import

class PGCRISO(ApplicationBase):
    '''
    Handles the ISO files
    '''
    def __init__(self):
        '''
        Class Constructor
        '''
        super(PGCRISO, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def DownloadFilesFromS3(self):
        '''
        Downloads all files from S3
        '''
        for iso in self.job["iso_files"]:
            keys = S3Utilities.GetListOfFiles(self.awsParams.s3, self.job["bucketName"], self.job["s3SrcDirectory"][1:] + iso["Name"] + "/")
            for key in keys:
                s3Key = "/" + key
                FileUtilities.CreateFolder(self.localTempDirectory + "/" + iso["Name"] + "/")
                localGzipFilepath = self.localTempDirectory + "/" + iso["Name"] + "/" + key.split("/")[-1]
                self.DownloadFile(s3Key, localGzipFilepath)

    def DownloadFile(self, s3Key, localGzipFilepath):
        '''
        Worker function to download the file
        '''
        self.logger.info(self.moduleName + " Downloading file: " + s3Key)
        try:
            S3Utilities.DownloadFileFromS3(self.awsParams.s3, self.job["bucketName"], s3Key, localGzipFilepath)
        except Exception as ex:
            self.logger.exception("Error while downloading file: {}".format(s3Key))
            self.logger.exception("{}".format(str(ex)))
            raise

    def Start(self, logger, moduleName, filelocs):
        '''
        Application starting point
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.DownloadFilesFromS3()
            ISO(logger, self.fileUtilities, self.localTempDirectory, self.job, self.awsParams) #invoke the ISO handler
        except Exception as ex:
            self.logger.exception(moduleName + " - Exception!")
            self.logger.exception("{}".format(str(ex)))
            raise
