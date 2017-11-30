'''
Created on Oct 13, 2017

@author: VIU53188
@license: IHS - not to be used outside the company
'''

import os
import sys
import shutil

from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.ExcelUtilities import ExcelUtilities

from Applications.Common.ApplicationBase import ApplicationBase

class HistoricalBrent(ApplicationBase):
    '''
    Code to process Historical Brent file from network share
    '''

    def __init__(self):
        '''
        Initial settings
        '''
        super(HistoricalBrent, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))

    def DownloadFile(self, rawFolder):
        '''
        Download the file into the local folder.
        '''
        self.logger.debug(self.moduleName + " -- " + "DownloadFile starting ")
        fileNameDest = None
        flist = []
        try:
            rawFolder = self.localTempDirectory + '/raw/'
            for fnl in self.fileUtilities.ScanFolder(self.job["srcSharedFolder"]):
                
                fileNameDest = os.path.join(rawFolder + fnl)                
                self.logger.info(self.moduleName + " - Copying file from source folder...")
                shutil.copyfile(os.path.join(self.job["srcSharedFolder"], fnl), fileNameDest)
                fnamesplitarray = fnl.rsplit('.', 1)
                fExt = fnamesplitarray[1]
                if fExt == 'zip':
                    fnameLocation = rawFolder + fnl
                    self.fileUtilities.UnzipUsing7z(fnameLocation, rawFolder)
                    self.fileUtilities.DeleteFile(fnameLocation)
                    flist = self.fileUtilities.ScanFolder(rawFolder)
                
                
        except:
            self.logger.exception(self.moduleName + "- we had an error in getxlFilename ")
            raise 
        finally:
            self.logger.debug(self.moduleName + " -- " + "DownloadFile finished ")
        return flist
    def LoadData(self, tblJson):
        '''
        load the csv file(s) to s3 and then into RedShift 
        '''    
        try:
            self.logger.debug(self.moduleName + " -- " + "LoadData" + " starting ")
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

            s3folder = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"]
            fldDelimiter = self.job["delimiter"]
            if "delimiter" in tblJson:
                fldDelimiter = tblJson["delimiter"]
            ignorheader = 0
            if "header" in tblJson:
                ignorheader = tblJson["header"]

            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": tblJson["schemaName"],
                                                 "tableName": tblJson["table"],
                                                 "s3Filename": s3folder,
                                                 "fileFormat": self.job["fileFormat"],
                                                 "dateFormat": self.job["dateFormat"],
                                                 "delimiter": fldDelimiter,
                                                 "ignoreheader": ignorheader
                                             },
                                             self.logger, "N")
            self.logger.info(self.moduleName + " - Finished loading s3 data to " +\
                             tblJson["schemaName"] + '.' +  tblJson["table"])
            rsConnect.close()
            self.logger.debug(self.moduleName + " -- " + "LoadData" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in LoadData")
            raise

    def BulkUploadToS3(self):
        '''
        Uploads all GZIP files created into S3 to be uploaded later...
        '''
        self.logger.info(self.moduleName + " - Uploading GZIP files to s3 folder...")
        s3Location = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"] 
        S3Utilities.CopyItemsAWSCli(self.fileUtilities.gzipFolder,
                                    s3Location,
                                    "--recursive --quiet")

    def ProcessRequest(self):
        '''
        process steps:
        pulls file from share and place in raw folder
        '''
        try:
            rawFolder = self.localTempDirectory + '/raw/'
            flist = self.DownloadFile(rawFolder)
            xl = ExcelUtilities(self.logger)

            outPutFileName = self.fileUtilities.csvFolder + self.moduleName + '.csv'
            for fl in flist:
                xl.Excel2CSV(rawFolder + fl,\
                            'Sheet1',\
                            outPutFileName,\
                            self.fileUtilities.csvFolder)
                outputGZ = self.fileUtilities.gzipFolder + self.moduleName + '.csv.gz'
                
                self.fileUtilities.GzipFile(outPutFileName, outputGZ)
                self.fileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder)
                
            for tables in self.job["tables"]:
                fname = self.fileUtilities.CreateTableSql(tables, self.fileUtilities.sqlFolder)
                RedshiftUtilities.PSqlExecute(fname, self.logger)   
            self.BulkUploadToS3()
            self.LoadData(tables)         
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessRequest")
            raise
        
    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
            self.ProcessRequest()
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)                
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
