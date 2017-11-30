'''
Main script to load data from s3 into RedShift using a JSON configuration file
Author - Christopher Lewis
License: IHS - not to be used outside the company
'''
import os

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class AutoParc(ApplicationBase):
    '''
    Code to process the Automotive PARC data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(AutoParc, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))

    def LoadDataIntoRedShift(self):
        '''
        Connect to the Redshift database
        '''
        self.logger.debug(self.moduleName + " -- " + "LoadDataIntoRedShift " + " starting ")

        rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)
        RedshiftUtilities.LoadDataFromS3(
            rsConnect, self.awsParams.s3, self.job, self.logger)
        rsConnect.close()
        self.logger.debug(self.moduleName + " -- " + "LoadDataIntoRedShift " + " finished ")

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
            self.LoadDataIntoRedShift()
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)              
            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
