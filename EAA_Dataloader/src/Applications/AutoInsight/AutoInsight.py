'''
Created on Sep 27, 2017

@author: Hector Hernandez
@summary: Loads the AutoInsight Scenario data.

'''

import os
import sys
import shutil
import calendar
import pandas

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from Applications.Common.ApplicationBase import ApplicationBase
from xlrd import XLRDError

class AutoInsight(ApplicationBase):
    '''
    This class is used to control the data load process for Auto Insight.
    '''

    def __init__(self):
        '''
        Initial settings
        '''
        super(AutoInsight, self).__init__()

        self.awsParams = ""
        self.processingFile = None
        self.fileUtilities = FileUtilities(self.logger)
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def BulkDownload(self):
        '''
        Download all files.
        '''
        sharedFiles = self.fileUtilities.ScanFolder(self.job["srcSharedFolder"])

        self.logger.info(self.moduleName + " - Downloading files from shared folder...")

        for fileName in sharedFiles:
            if fileName == self.job["fileName"]:
                self.processingFile = fileName
                shutil.copyfile(os.path.join(self.job["srcSharedFolder"], fileName), self.localTempDirectory + "/" + self.processingFile)

    def LoadAllFromS3(self):
        '''
        Process a single category configured in the categories dictionary in the jobConfig.
        '''
        try:
            s3DataFolder = "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"]

            rsConnect = RedshiftUtilities.Connect(dbname=self.awsParams.redshift['Database'],
                                                  host=self.awsParams.redshift['Hostname'],
                                                  port=self.awsParams.redshift['Port'],
                                                  user=self.awsParams.redshiftCredential['Username'],
                                                  password=self.awsParams.redshiftCredential['Password'])

            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": self.job["destinationSchema"],
                                                 "tableName": self.job["ddl"]["table"],
                                                 "s3Filename": s3DataFolder,
                                                 "fileFormat": self.job["fileFormat"],
                                                 "dateFormat": self.job["dateFormat"],
                                                 "delimiter": self.job["delimiter"]
                                             },
                                             self.logger, "N")

            self.logger.info(self.moduleName + " - Cleaning s3 data folder...")

            S3Utilities.DeleteFileFromS3TempUsingAWSCLi(s3DataFolder, "--recursive --quiet")
        except Exception:
            self.logger.error(self.moduleName + " - Error while trying to save into Redshift from s3 folder.")
            raise

    @staticmethod
    def FormatColNameDate(dtText):
        '''
        Converts the abbreviated date to YYYY-MM-01 format
        '''
        textFixed = None
        if sys.version[0] == '2':
            customException = StandardError()
        elif sys.version[0] == '3':
            customException = Exception()
        
        try:
            textFixed = str(dtText[4:]) + "-" + str(list(calendar.month_abbr).index(dtText[:3])) + "-01"
        except customException:
            textFixed = dtText

        return textFixed

    def ProcessFiles(self):
        '''
        Controls the workflow for the conversion, clean up and pack of the input files.
        '''
        self.logger.info(self.moduleName + " - Processing file: " + self.processingFile)

        rawFileName = self.localTempDirectory + "/" + self.processingFile
        csvFilename = self.localTempDirectory + "/" + self.processingFile.split(".")[0] + ".csv"

        try:
            columnNames = []

            df = pandas.read_excel(rawFileName,
                                   sheetname=self.job["worksheetName"],
                                   index_col=None,
                                   na_values=None,
                                   skiprows=self.job["skipRows"],
                                   skip_footer=self.job["skipFooter"])

            for colName in df.head(0):
                if colName not in self.job["columns_no_melt"]:
                    columnNames.append(self.FormatColNameDate(colName))
                else:
                    columnNames.append(colName)

            df.columns = columnNames
            df = df.melt(id_vars=self.job["columns_no_melt"])

            df.to_csv(csvFilename,
                      header=False,
                      sep=str(self.job["delimiter"]),
                      encoding='utf-8',
                      index=False)

            self.fileUtilities.GzipFile(csvFilename, csvFilename + ".gz")
            self.fileUtilities.DeleteFile(csvFilename)
        except XLRDError:
            self.logger.info(self.moduleName + " - No tab named '" + self.job["worksheetName"] + "' in " + self.processingFile)
        except Exception:
            self.logger.error(self.moduleName + " - Error while trying to process file " +  self.processingFile)
            raise
        finally:
            FileUtilities.RemoveFileIfItExists(rawFileName)

    def BulkUploadToS3(self):
        '''
        Uploads all GZIP files created into S3 to be uploaded later...
        '''
        self.logger.info(self.moduleName + " - Uploading GZIP files to s3 folder...")

        fileName = self.processingFile.split(".")[0] + ".csv.gz"
        S3Utilities.CopyItemsAWSCli(self.localTempDirectory + "/" + fileName,
                                    "s3://" + self.job["bucketName"] + self.job["s3ToDirectory"], "--quiet")

    def ExecuteCreateTable(self):
        '''
        Checks if the tables needs to be created
        '''
        tb = self.job['ddl']
        tb['schemaName'] = self.job['destinationSchema']

        fname = self.fileUtilities.CreateTableSql(tb, self.localTempDirectory)
        RedshiftUtilities.PSqlExecute(fname, self.logger)

    def Start(self, logger, moduleName, filelocs):
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)

            self.ExecuteCreateTable()
            self.BulkDownload()
            self.ProcessFiles()
            self.BulkUploadToS3()
            self.LoadAllFromS3()

            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)

            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)

            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"], currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")

            raise Exception()
