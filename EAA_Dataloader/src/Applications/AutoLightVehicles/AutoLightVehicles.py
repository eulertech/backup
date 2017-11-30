'''
Main script to process the EIA data
Author - Christopher Lewis
License: IHS - not to be used outside the company
'''

import os
import ntpath
import re
#import pandas as pd


from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
#from AACloudTools.PandasUtilities import PandasUtilities
from AACloudTools.ExcelUtilities import ExcelUtilities

from Applications.Common.ApplicationBase import ApplicationBase

class AutoLightVehicles(ApplicationBase):
    '''
    Code to process the Auto Light Vehicles data
    '''

    def __init__(self):
        '''
        Initial settings
        '''
        super(AutoLightVehicles, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))

    # -----------------------------------------------------------------------------
    def ProcessS3File(self, srcFileParameter):
        '''
        Process each file
        '''
        self.logger.debug(self.moduleName + " -- " + "ProcessS3File" + " starting ")
        s3Key = self.job["s3SrcDirectory"] + "/" + srcFileParameter["s3Filename"]
        self.logger.info(self.moduleName + " - Processing file: " + s3Key)

        fileName = ntpath.basename(s3Key)
        localGzipFilepath = self.localTempDirectory + "/raw/" + fileName

        #----------------------------------------------------------------------
        S3Utilities.DownloadFileFromS3(
            self.awsParams.s3, self.job["bucketName"], s3Key, localGzipFilepath)

        # Remove the gz extension
        localExcelFilepath = re.sub(r'\.gz$', '', localGzipFilepath)
        self.fileUtilities.GunzipFile(localGzipFilepath, localExcelFilepath)

        self.logger.info(self.moduleName +
                         " - Processing Excel file: " + localExcelFilepath)
        self.fileUtilities.DeleteFile(localGzipFilepath)
        fileNameNoExt = fileName.split('.', 1)[0]        
        outPutFileName = self.fileUtilities.csvFolder + fileNameNoExt + '.csv'
        xl = ExcelUtilities(self.logger)
        xl.Excel2CSV(localExcelFilepath,\
                    srcFileParameter["excelSheetName"],\
                    outPutFileName,\
                    self.fileUtilities.csvFolder,\
                    skiprows=srcFileParameter["skipRows"])
        self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/raw/")
        for tables in srcFileParameter["tables"]:
            fname = self.fileUtilities.CreateTableSql(tables, self.fileUtilities.sqlFolder)
            RedshiftUtilities.PSqlExecute(fname, self.logger)   
        # -----------------------------------------------------------------------------
        self.logger.info(self.moduleName + " - Loading data into Redshift...")
        rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

        RedshiftUtilities.LoadFileIntoRedshift(rsConnect, self.awsParams.s3, self.logger, 
                                               self.fileUtilities, outPutFileName,
                                               tables["schemaName"], tables["table"],
                                               self.job["fileFormat"], self.job["dateFormat"],
                                               self.job["delimiter"])
        # Cleanup
        rsConnect.close()
        self.fileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder)
        
        self.logger.debug(self.moduleName + " -- " + "ProcessS3File for file: " + s3Key + " finished ")

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
            for srcFileParameter in self.job["srcFileParameters"]:
                self.ProcessS3File(srcFileParameter)
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)                
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
