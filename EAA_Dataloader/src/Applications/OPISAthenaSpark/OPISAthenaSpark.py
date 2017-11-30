'''
Main script to process the OPIS data
@author:  - viu53188
@summary: Modified from OPIS to use Athena Spark framework
@license: IHS - not to be used outside the company
'''

import os

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class OPISAthenaSpark(ApplicationBase):
    '''
    Code to process the PGCR Air Markets data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(OPISAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))

    def DownloadFilesFromS3(self, tablesJson):
        '''
        Download files from the s3 data folder.
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "DownloadFileFromS3" + " starting " + tablesJson["srcFile"])
            S3Utilities.CopyItemsAWSCli("s3://" + tablesJson["srcBucketName"] + tablesJson["srcS3DataFolder"] + tablesJson["srcFile"],
                                        self.fileUtilities.csvFolder,
                                        "--quiet")
            self.logger.debug(self.moduleName + " -- " + "DownloadFileFromS3" + " finished " + tablesJson["srcFile"])
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to download file from s3. Error: " + err.message)
            raise

    def ProcessTables(self, dbCommon, tables):
        '''
        process steps:
        pulls file from share and place in raw folder
        '''
        try:
            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            self.fileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder)
            if "srcS3DataFolder" in tables:
                self.DownloadFilesFromS3(tables)
            df = SparkUtilities.ReadCSVFile(spark, tables, tables["delimiter"], False,
                                            self.fileUtilities.csvFolder, self.logger)
            if "adjustFormat" in tables:
                for fld in tables["adjustFormat"]:
                    df = SparkUtilities.FormatColumn(df, fld["name"], fld["inputFormat"])
            #  remove any null records
            df = df.dropna(how='all')
            SparkUtilities.SaveParquet(df, self.fileUtilities)
            self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet, None)
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
