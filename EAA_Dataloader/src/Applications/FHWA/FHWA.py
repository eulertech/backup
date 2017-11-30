'''
Created on Mar 22, 2017

@author: Hector Hernandez
@summary: Loads the OPEC Calendar data excel file pulled from the OPEC web site.

'''

import os
import urllib

from Applications.Common.ApplicationBase import ApplicationBase
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.DBFUtilities import DBFUtilities

class FHWA(ApplicationBase):
    '''
    This class is used to control the data load process from different OPEC file sources.
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(FHWA, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))

    def DownloadFile(self, srcCategory):
        '''
        Download the file into the local folder.
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "DownloadFile" + " starting ")

            localFilepath = self.localTempDirectory + "/" + srcCategory["fileName"]

            fileDownload = urllib.URLopener()
            fileDownload.retrieve(srcCategory["srcUrl"] + "/" + srcCategory["fileName"], localFilepath)
            self.logger.debug(self.moduleName + " -- " + "DownloadFile" + " finished ")

            return localFilepath
        except Exception as err:
            self.logger.info(err.message)
            raise Exception(err.message)

    def LoadCategory(self, srcCategory):
        '''
        Process a single category configured in the categories dictionary in the jobConfig.
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "LoadCategory" + " starting ")
            processingFile = self.DownloadFile(srcCategory)
            fileOut = processingFile.replace(".dbf", ".txt")

            dbfUtils = DBFUtilities(self.logger)
            dbfUtils.ConvertToCSV(processingFile, fileOut, srcCategory["delimiter"], False)

            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)
            self.logger.info(self.moduleName + " - Loading file " + fileOut + "...")

            RedshiftUtilities.LoadFileIntoRedshift(rsConnect,
                                                   self.awsParams.s3,
                                                   self.logger,
                                                   self.fileUtilities,
                                                   fileOut,
                                                   self.job["destinationSchema"],
                                                   self.job["tableName"] + "_" + srcCategory["srcCategory"],
                                                   srcCategory["fileFormat"],
                                                   srcCategory["dateFormat"],
                                                   srcCategory["delimiter"])
            self.logger.debug(self.moduleName + " -- " + "LoadCategory" + " finished ")
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to load category...")
            raise Exception(err.message)

    def ProcessCategories(self):
        '''
        Controls the flow execution of all categories configured.
        '''
        self.logger.debug(self.moduleName + " -- " + "ProcessCategories" + " starting ")
        for srcCategory in self.job["srcCategories"]:
            if srcCategory["execute"] == "Y":
                self.LoadCategory(srcCategory)
        self.logger.debug(self.moduleName + " -- " + "ProcessCategories" + " finished ")

    def Start(self, logger, moduleName, filelocs):
        '''
        main routine
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
            self.ProcessCategories()
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)
            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
