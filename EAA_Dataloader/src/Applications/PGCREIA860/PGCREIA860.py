'''
Main script to process the PGCR EIA 860 data
Author - Chinmay
License: IHS - not to be used outside the company
'''

import os
import re

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.ExcelUtilities import ExcelUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class PGCREIA860(ApplicationBase):
    '''
    Code to process the PGCR EIA Form - 923 data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(PGCREIA860, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))
        self.awsParams = ""
        self.excelUtilities = None

    def PostLoadETL(self):
        '''
        Create the L2 tables post data load
        '''
        postLoadScripts = self.job.get("PostLoadScript")
        if postLoadScripts is not None: #there could be more than one script
            for postLoadScript in postLoadScripts: #get each script at a time
                if postLoadScript is not None:
                    sqlTableCreationScript = super(PGCREIA860, self).BuildTableCreationScript(postLoadScript)
                    RedshiftUtilities.PSqlExecute(sqlTableCreationScript, self.logger)
                    self.logger.info(self.moduleName + " - SQL tables created.")

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.excelUtilities = ExcelUtilities(logger)
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory) #delete and recreate the folder
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/CSVS/") #delete and recreate the folder
            self.BulkDownload()
            self.UnzipExcel()
            for sheetParams in self.job["ExcelSheets"]:
                self.Excel2CsvSkipped(sheetParams)
            self.PostLoadETL()
            self.fileUtilities.EmptyFolderContents(self.localTempDirectory) #clear contents of the folder
        except Exception:
            logger.exception(moduleName + " - Exception!")
            raise

    def BulkDownload(self):
        '''
        Download the entire bucket of EIA 860
        '''
        for path in self.job["s3SrcDirectory"]:
            try:
                sourcePath = "s3://" + self.job["bucketName"] + "/" + path
                outputPath = self.localTempDirectory + "/"
                S3Utilities.CopyItemsAWSCli(sourcePath, outputPath, "--recursive --quiet")
            except:
                self.logger.exception("Exception in PGCREIA860.BulkDownload. Location {}".format(sourcePath))
                raise


    def UnzipExcel(self):
        '''
        Convert zip files to Excel worksheets using FileUtilities
        '''
        try:
            fullPath = self.localTempDirectory
            listOfFiles = self.fileUtilities.ScanFolder(fullPath)
            for zipFile in listOfFiles:
                if zipFile.lower().endswith(".zip"):
                    localFilePath = fullPath + "/" + zipFile
                    outFilePath = fullPath
                    self.fileUtilities.UnzipFile(localFilePath, outFilePath)
        except:
            self.logger.exception("Exception in PGCREIA860.UnzipExcel")
            raise

    def Excel2CsvSkipped(self, sheetParams):
        '''
        Convert Excel worksheets to csvs using Excel Utilities
        '''
        try:
            fileNames = self.fileUtilities.ScanFolder(self.localTempDirectory)
            rg = re.compile(sheetParams["FileRegex"])
            fileName = list(filter(rg.match, fileNames))
            localFilePath = self.localTempDirectory + "/CSVS/"
            csvName = self.localTempDirectory + "/CSVS/" + sheetParams["redshiftTableSuffix"] + ".csv"
            self.excelUtilities.Excel2CSV(self.localTempDirectory + "/" + fileName[0], sheetParams["Sheet"], csvName, localFilePath, ',')
            self.logger.info(fileName[0] + " -> " + csvName)
            self.fileUtilities.RemoveLines(csvName, sheetParams["Skip"])
            self.LoadCSVFile(csvName, sheetParams)
            self.fileUtilities.DeleteFile(csvName) #delete the csv file
        except:
            self.logger.exception("Exception in PGCREIA860.Excel2CsvSkipped")
            raise

    def LoadCSVFile(self, localFilePath, loadName):
        '''
        For each file we need to process, provide the data loader the s3 key
        and destination table name
        '''
        self.logger.info("Loading data into Redshift")
        try:
            rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                                  host=self.awsParams.redshift['Hostname'],
                                                  port=self.awsParams.redshift['Port'],
                                                  user=self.awsParams.redshiftCredential['Username'],
                                                  password=self.awsParams.redshiftCredential['Password'])

            RedshiftUtilities.LoadFileIntoRedshift(rsConnect,
                                                   self.awsParams.s3,
                                                   self.logger,
                                                   self.fileUtilities,
                                                   localFilePath,
                                                   self.job["destinationSchema"],
                                                   self.job["tableName"] + loadName["redshiftTableSuffix"],
                                                   self.job["fileFormat"],
                                                   self.job["dateFormat"],
                                                   self.job["delimiter"])
        except Exception:
            self.logger.exception("Exception in PGCREIA860.LoadCSVFile")
            self.logger.exception("Error while uploading to table:{}, filePath:{}".format(self.job["tableName"] + loadName["redshiftTableSuffix"],
                                                                                          localFilePath))
            raise
        finally:
            if rsConnect is not None:
                rsConnect.close()
            self.logger.info(self.moduleName + " - Finished Processing S3 file: " + loadName["redshiftTableSuffix"])
