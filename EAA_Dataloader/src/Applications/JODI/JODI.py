'''
Created on Mar 22, 2017

@author: Hector Hernandez
@summary: Loads files pulled from JODI web sites based on year
        in the initial load these values are passed in the form of a json config object
'''

import os
import ntpath
import urllib

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class JODI(ApplicationBase):
    '''
    Download and process JODI's Primary and Secondary data coming from their web site...
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(JODI, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def GetOutputScript(self, scriptName):
        '''
        Generates an output script for the references tables.
        '''
        self.logger.debug(self.moduleName + " - GetOutputScript for " + scriptName + " starting...")

        sqlTemplateFile = self.fileUtilities.PathToForwardSlash(os.path.join(self.location, self.job["dmlFolder"], scriptName))
        sqlOutputFile = self.fileUtilities.PathToForwardSlash(os.path.join(self.localTempDirectory, scriptName))

        self.fileUtilities.CreateActualFileFromTemplate(sqlTemplateFile, sqlOutputFile, self.job["destinationSchema"], "")

        return sqlOutputFile

    def ProcessReferenceData(self):
        '''
        Process each reference data...
        '''
        try:
            self.logger.debug(self.moduleName + " - " + "ProcessReferenceData" + " starting ")

            for refTable in self.job["referenceData"]["tables"]:
                RedshiftUtilities.PSqlExecute(self.GetOutputScript(refTable["ddlScriptName"]), self.logger)
        except StandardError:
            self.logger.error(self.moduleName + ' - Error in process reference data.')
            raise

    def ProcessCategory(self, rsConnect, srcCategory):
        '''
        Executes the processing for a single category configured...
        '''
        url = srcCategory["url"]
        self.logger.info(self.moduleName + " - Processing url: " + url)

        localFilepath = self.localTempDirectory + "/" + ntpath.basename(srcCategory["url"])

        fileDownload = urllib.URLopener()
        fileDownload.retrieve(url, localFilepath)

        self.fileUtilities.UnzipFile(localFilepath, self.localTempDirectory)
        localFilepath = self.localTempDirectory + "/" + srcCategory["unzipFilename"]

        redshiftDestTable = self.job["tableName"] + srcCategory["redshiftTableSuffixOrigin"]

        RedshiftUtilities.LoadFileIntoRedshift(rsConnect,
                                               self.awsParams.s3,
                                               self.logger,
                                               self.fileUtilities,
                                               localFilepath,
                                               self.job["destinationSchema"],
                                               redshiftDestTable,
                                               self.job["fileFormat"],
                                               srcCategory["dateFormat"],
                                               self.job["delimiter"])

    def CreateDataViews(self):
        '''
        Executes the view creation in redshift....
        '''
        try:
            self.logger.info(self.moduleName + " - Creating View...")

            RedshiftUtilities.PSqlExecute(self.GetOutputScript(self.job["views"]["createScriptName"]), self.logger)
        except Exception as err:
            self.logger.error(self.moduleName + ' - Error while trying to Create data view. Message:' + err.message)
            raise

    def ProcessCategories(self):
        '''
        Controls the processing of each category configured...
        '''
        rsConnect = None

        try:
            self.logger.debug(self.moduleName + " -- " + "ProcessCategories" + " starting ")
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

            for srcCategory in self.job["srcCategories"]:
                self.ProcessCategory(rsConnect, srcCategory)
        except Exception as err:
            self.logger.error(self.moduleName + ' - Error while processing categories. Message:' + err.message)
            raise
        finally:
            if rsConnect is not None:
                rsConnect.close()

    def Start(self, logger, moduleName, filelocs):
        '''
        Main control of JODI's ETL process...
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " - " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
            self.ProcessCategories()
            self.ProcessReferenceData()
            self.CreateDataViews()
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
