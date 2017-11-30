'''
Created on Oct 31, 2017

@author: Hector Hernandez
@summary: Loads the FHWA data using Spark.

'''
import os
import urllib

from Applications.Common.ApplicationBase import ApplicationBase
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.DBFUtilities import DBFUtilities
from AACloudTools.SparkUtilities import SparkUtilities

class FHWAAthenaSpark(ApplicationBase):
    '''
    This class is used to control the data load process from different OPEC file sources.
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(FHWAAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def DownloadFile(self):
        '''
        Download the file into the local folder.
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "DownloadFile" + " starting ")

            localFilepath = self.localTempDirectory + "/" + self.job["fileName"]

            fileDownload = urllib.URLopener()
            fileDownload.retrieve(self.job["srcUrl"] + "/" + self.job["fileName"], localFilepath)

            return localFilepath
        except Exception as err:
            self.logger.info(err.message)
            raise Exception(err.message)

    def ProcessTables(self, dbCommon, tables):
        '''
        Process a single category configured in the categories dictionary in the jobConfig.
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "LoadCategory" + " starting ")
            processingFile = self.DownloadFile()
            fileOut = processingFile.replace(".dbf", ".txt")
            
            dbfUtils = DBFUtilities(self.logger)
            dbfUtils.ConvertToCSV(processingFile, fileOut, self.job["delimiter"], False)
            
            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            schema = SparkUtilities.BuildSparkSchema(tables)
            df = (spark.read
                     .format("com.databricks.spark.csv")
                     .options(header='false', delimiter=self.job["delimiter"])
                     .schema(schema)
                     .load(fileOut)
                     )
            self.logger.info(self.moduleName + " -- " + "Done reading " + str(df.count()) + " rows.  Now saving as parquet file...")
            SparkUtilities.SaveParquet(df, self.fileUtilities)
            self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet)
            self.LoadDataFromAthenaIntoRedShiftLocalScripts(tables)
            self.logger.debug(self.moduleName + " -- " + "LoadCategory" + " finished ")
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to load category...")
            raise Exception(err.message)

    def Start(self, logger, moduleName, filelocs):
        '''
        main routine
        '''
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)