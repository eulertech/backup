'''
Created on Nov 9, 2017

@author: Hector Hernandez
@summary: Extracts and transforms Vantage data from Vantage SQL DB.
'''
import os

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class VantageAthenaSpark(ApplicationBase):
    '''
    This class is used to get the Vantage data from IHS Vantage Database, and load it into Athena.
    '''

    def __init__(self):
        '''
        Initial settings
        '''
        super(VantageAthenaSpark, self).__init__()

        self.fileUtilities = FileUtilities(self.logger)
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def BulkExtract(self, datasetName, scriptName, common):
        '''
        Bulk extracts the different data sets coming from Vantage DB.
        '''
        try:
            fileName = self.localTempDirectory + "/csv/" + datasetName + ".CSV"

            self.bcpUtilities.RunBCPJob(common["mssqlLoginInfo"],
                                        self.job["bcpUtilityDirOnLinux"],
                                        self.fileUtilities.LoadSQLQuery(self.location + scriptName),
                                        fileName,
                                        self.job["delimiter"])

            return fileName
        except Exception as err:
            self.logger.error("Error while trying to Bulk Extract. Message: " + err.message)
            raise

    def ProcessTables(self, dbCommon, tables):
        '''
        Process each Vantage table.
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "Processing data for table:" + tables["table"])
            fileName = self.BulkExtract(tables["table"], tables["scriptFile"], dbCommon)

            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            schema = SparkUtilities.BuildSparkSchema(tables)
            df = (spark.read
                  .format("com.databricks.spark.csv")
                  .options(header='false', delimiter=self.job["delimiter"])
                  .schema(schema)
                  .load(fileName)
                 )

            self.logger.info(self.moduleName + " -- " + "Done reading " + str(df.count()) + " rows.  Now saving as parquet file...")
            SparkUtilities.SaveParquet(df, self.fileUtilities)
            self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet)
            self.LoadDataFromAthenaIntoRedShiftLocalScripts(tables)
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to load table. Error: " + err.message)
            raise

    def Start(self, logger, moduleName, filelocs):
        '''
        main routine
        '''
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)
