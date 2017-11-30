'''
Main script to process the PGCR Air Markets data
Author - Christopher Lewis using Chinmay's scripts for json and SQL
         Code based on the OPIS template
License: IHS - not to be used outside the company
'''

import os

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class PGCRAirMarkets(ApplicationBase):
    '''
    Code to process the PGCR Air Markets data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(PGCRAirMarkets, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))
        self.awsParams = ""

    def ProcessS3File(self, srcFileParameter):
        '''
        For each file we need to process, provide the data loader the s3 key
        and destination table name
        '''
        jobParams = dict(self.job)
        jobParams["s3Filename"] = "s3://" + self.job["bucketName"] + "/" + \
            self.job["s3SrcDirectory"] + "/" + srcFileParameter["s3Filename"]
        jobParams["tableName"] = self.job["tableName"] + \
            srcFileParameter["redshiftTableSuffix"]
        self.logger.info(self.moduleName +
                         " - Processing S3 file: " + jobParams["s3Filename"])

        rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                              host=self.awsParams.redshift['Hostname'],
                                              port=self.awsParams.redshift['Port'],
                                              user=self.awsParams.redshiftCredential['Username'],
                                              password=self.awsParams.redshiftCredential['Password'])

        RedshiftUtilities.LoadDataFromS3(
            rsConnect, self.awsParams.s3, jobParams, self.logger)
        rsConnect.close()
        self.logger.info(
            self.moduleName + " - Finished Processing S3 file: " + jobParams["s3Filename"])

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            for srcFileParameter in self.job["srcFileParameters"]:
                self.ProcessS3File(srcFileParameter)
        except:
            logger.exception(moduleName + " - Exception!")
            raise
