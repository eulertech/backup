'''
Main script to process the OPIS data
Author - Christopher Lewis
License: IHS - not to be used outside the company
'''

import os

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class OPIS(ApplicationBase):
    '''
    Code to process the PGCR Air Markets data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(OPIS, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))

    def ProcessS3File(self, srcFileParameter):
        '''
        For each file we need to process, provide the data loader the s3 key
        and destination table name
        '''
        self.logger.debug(self.moduleName + " -- " + "ProcessS3File for " + srcFileParameter["s3Filename"] + " starting ")
        jobParams = dict(self.job)
        jobParams["s3Filename"] = "s3://" + self.job["bucketName"] + "/" + \
            self.job["s3SrcDirectory"] + "/" + srcFileParameter["s3Filename"]
        jobParams["tableName"] = self.job["tableName"] + \
            srcFileParameter["redshiftTableSuffix"]
        self.logger.debug(self.moduleName + " - Processing S3 file: " + jobParams["s3Filename"])

        rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

        RedshiftUtilities.LoadDataFromS3(rsConnect,
                                         self.awsParams.s3,
                                         jobParams,
                                         self.logger)
        rsConnect.close()
        self.logger.debug(self.moduleName + " -- " + "ProcessS3File for " + srcFileParameter["s3Filename"] + " finished ")

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
            for srcFileParameter in self.job["srcFileParameters"]:
                self.ProcessS3File(srcFileParameter)
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)
            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
