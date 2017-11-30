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

class AthenaSparkIncrementalExample(ApplicationBase):
    '''
    Code to process the Automotive PARC data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(AthenaSparkIncrementalExample, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))


    def ProcessTable(self, table):
        '''
        Process the data for the table
        '''
        s3Key = table["s3Filename"]
        self.logger.info(self.moduleName + " - Processing file: " + s3Key)

        self.CreateFolders(table["table"])
            
        fileName = ntpath.basename(s3Key)
        localTxtFilepath = self.fileUtilities.csvFolder + "/" + fileName
        S3Utilities.DownloadFileFromS3(self.awsParams.s3, self.job["bucketName"],
                                       s3Key, localTxtFilepath)

        spark = SparkUtilities.GetCreateSparkSession(self.logger)
        schema = SparkUtilities.BuildSparkSchema(table)
        df = (spark.read
                 .format("com.databricks.spark.csv")
                 .options(header='false', delimiter=self.job["delimiter"])
                 .schema(schema)
                 .load(localTxtFilepath)
                 )
        self.logger.info(self.moduleName + " -- " + "Done reading " + str(df.count()) + " rows.  Now saving as parquet file...")
        SparkUtilities.SaveParquet(df, self.fileUtilities)
        self.UploadFilesCreateAthenaTablesAndSqlScripts(table, self.fileUtilities.parquet, table["partitionValue"])
        self.logger.info(self.moduleName + " -- " + "ProcessTable " + " finished ")

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
            
            for table in self.job["tables"]:
                self.ProcessTable(table)
                self.LoadDataFromAthenaIntoRedShiftLocalScripts(table)
                #self.LoadDataFromAthenaIntoRedShiftS3Scripts(table) # Test: Load all data from Athena
                
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)              
            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
