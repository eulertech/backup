'''
Created on Nov 22, 2017

@author: viu53188
@summary: Loads files pulled from JODI web sites based on year
        in the initial load these values are passed in the form of a json config object
'''

import os
import ntpath
import urllib

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.ExcelUtilities import ExcelUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.SparkUtilities import SparkUtilities

from Applications.Common.ApplicationBase import ApplicationBase

class JODIAthenaSpark(ApplicationBase):
    '''
    Download and process JODI's Primary and Secondary data coming from their web site...
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(JODIAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def DownloadFilesFromS3(self, tablesJson):
        '''
        Download all files from the s3 data folder.
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "DownloadFileFromS3" + " starting " + tablesJson["srcFile"])
            rawFolder = self.localTempDirectory + '/raw/'
            S3Utilities.CopyItemsAWSCli("s3://" + tablesJson["srcBucketName"] + tablesJson["srcS3DataFolder"] + tablesJson["srcFile"],
                                        rawFolder,
                                        "--quiet")

            self.logger.debug(self.moduleName + " -- " + "DownloadFileFromS3" + " finished " + tablesJson["srcFile"])
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to download file from s3. Error: " + err.message)
            raise

    def ProcessWebCall(self, tables, rawFolder):
        '''
        Executes the processing for a single category configured...
        '''
        url = tables["url"]
        self.logger.info(self.moduleName + " - Processing url: " + url)

        localFilepath = rawFolder + ntpath.basename(tables["url"])

        fileDownload = urllib.URLopener()
        fileDownload.retrieve(url, localFilepath)

        self.fileUtilities.UnzipFile(localFilepath, self.fileUtilities.csvFolder)

    def ProcessTables(self, dbCommon, tables):
        '''
        process steps:
        pulls file from share and place in raw folder
        '''
        try:
            rawFolder = self.localTempDirectory + '/raw/'
            if "srcS3DataFolder" in tables:
                self.DownloadFilesFromS3(tables)
                xl = ExcelUtilities(self.logger)
                outPutFileName = self.fileUtilities.csvFolder + self.moduleName + '.csv'
                xl.Excel2CSV(rawFolder + tables["srcFile"],\
                            None,\
                            outPutFileName,\
                            self.fileUtilities.csvFolder,\
                            defDateFormat='%Y-%m-%d',\
                            skiprows=tables["skipRows"])
            else:
                self.ProcessWebCall(tables, rawFolder)

            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            df = SparkUtilities.ReadCSVFile(spark, tables, tables["delimiter"], False,
                                            self.fileUtilities.csvFolder, self.logger)
            if "adjustFormat" in tables:
                for fld in tables["adjustFormat"]:
                    df = SparkUtilities.FormatColumn(df, fld["name"], fld["inputFormat"])

            self.fileUtilities.EmptyFolderContents(rawFolder)
            #  remove any null records
            df = df.dropna(how='all')
            SparkUtilities.SaveParquet(df, self.fileUtilities)
            self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet)
            self.fileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder)
            if "loadToRedshift" in tables and tables["loadToRedshift"] == "Y":
                    self.LoadDataFromAthenaIntoRedShiftLocalScripts(tables)
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessRequest")
            raise

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)
