'''
Main script to load data from s3 into RedShift using a JSON configuration file
Author - Christopher Lewis
License: IHS - not to be used outside the company
'''
import os
import ntpath

from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class AutoParcAthenaSpark(ApplicationBase):
    '''
    Code to process the Automotive PARC data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(AutoParcAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))

    def ProcessTables(self, dbCommon, tables):
        '''
        Process the data for the table
        '''
        s3Key = self.job["s3Filename"]
        self.logger.info(self.moduleName + " - Processing file: " + s3Key)
        
        fileName = ntpath.basename(s3Key)
        localGzipFilepath = self.fileUtilities.gzipFolder + "/" + fileName
        S3Utilities.S3Copy(s3Key, localGzipFilepath)

        # Unzip the file rather than reading the gzip as Spark is faster with csv
        localCSVFilepath = self.fileUtilities.csvFolder + "/" + fileName + ".csv"
        self.fileUtilities.GunzipFile(localGzipFilepath, localCSVFilepath)

        spark = SparkUtilities.GetCreateSparkSession(self.logger)
        df = SparkUtilities.ReadCSVFile(spark, tables, self.job["delimiter"], True, self.fileUtilities.csvFolder, self.logger)
        SparkUtilities.SaveParquet(df, self.fileUtilities)
        self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet)
        self.LoadDataFromAthenaIntoRedShiftS3Scripts(tables)
        self.logger.info(self.moduleName + " -- " + "ProcessTable " + " finished ")

    def Start(self, logger, moduleName, filelocs):
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)
