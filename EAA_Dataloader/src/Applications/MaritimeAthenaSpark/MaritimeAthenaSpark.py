'''
Created on Aug 17, 2017

@author: VIU53188
@summary: Pulls Maritime data and loads into S3.
'''

import os
import datetime

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from AACloudTools.AthenaUtilities import AthenaUtilities
from Applications.Common.ApplicationBase import ApplicationBase
from AACloudTools.BCPUtilities import BCPUtilities

class MaritimeAthenaSpark(ApplicationBase):
    '''
    Class used to pull Maritime data.
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(MaritimeAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def UpdateChunkStartEnd(self, chunkEnd, chunkSize, maxValue):
        '''
        return the updated start and end indices
        '''
        chunkStart = chunkEnd + 1
        chunkEnd = chunkEnd + chunkSize
        if chunkEnd > maxValue:
            chunkEnd = maxValue
        return chunkStart, chunkEnd

    def FlushAndFillUsingJDBC(self, dbCommon, tables):
        '''
        Simple flush and fill.  Get the data from JDBC and load into Athena
        '''
        spark = SparkUtilities.GetCreateSparkSession(self.logger)
        url, driver = SparkUtilities.GetSqlServerConnectionParams(dbCommon)
        df = SparkUtilities.ReadTableUsingJDBC(spark, url, driver, tables, self.logger)
        SparkUtilities.SaveParquet(df, self.fileUtilities)
        self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet)
        self.LoadDataFromAthenaIntoRedShiftLocalScripts(tables)

    def IncrementalLoad(self, dbCommon, tables):
        self.fileUtilities.EmptyFolderContents(self.fileUtilities.sqlFolder)
        try:
            # This is where we last ended.  Start at 1 + this end
            athenaSchemaName = AthenaUtilities.ComposeAthenaSchemaName(tables["schemaName"])
            chunkEnd = int(AthenaUtilities.GetMaxValue(self.awsParams, athenaSchemaName, tables["table"],
                                                   tables["incrementalconditions"]["keyfield"], self.logger))
        except ValueError:
            chunkEnd = 0 # Table does not exist yet
        except:
            raise
                
        #chunkEnd = 2960000000
        #maxValue = 3708000000 # 2249000000 3708000000
        maxValue = BCPUtilities.GetMaxValueSQLServer(dbCommon, tables, self.logger)
            
        chunkSize = tables["incrementalconditions"]["chunksize"]
        chunkStart, chunkEnd = self.UpdateChunkStartEnd(chunkEnd, chunkSize, maxValue)
    
        fieldTerminator = self.job["fieldTerminator"]
        rowTerminator = None # Not using this. Stick with the default of CR/LF.  self.job["rowTerminator"]
    
        chunkStartData = chunkStart
        # Each ETL gets the same date so that we can do a smart insert based on ETL and chunkStartData
        partitionValue = datetime.datetime.strftime(datetime.date.today(), '%Y-%m-%d')
        while chunkStart <= maxValue:
            sqlPullDataScript = BCPUtilities.CreatePullScript(dbCommon, tables, chunkStart, chunkEnd,
                                                              self.logger, self.fileUtilities, self.location)
            # Construct a file name that is meaning full.  That is, it has the start and end IDs
            fileBaseName = tables["incrementalconditions"]["keyfield"] + "-" + BCPUtilities.ComponseRangeString(chunkStart, chunkEnd)
            outputCSV = self.fileUtilities.csvFolder + fileBaseName + ".csv"
            self.fileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder)
            self.bcpUtilities.BulkExtract(sqlPullDataScript, outputCSV, dbCommon, tables, fieldTerminator, rowTerminator,
                                          self.job["bcpUtilityDirOnLinux"], self.fileUtilities, self.logger)
        
            # Process the data using Spark and save as Parquet
            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            df = SparkUtilities.ReadCSVFile(spark, tables, fieldTerminator, False,
                                            self.fileUtilities.csvFolder, self.logger)
            SparkUtilities.SaveParquet(df, self.fileUtilities, fileBaseName)
            self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet, partitionValue)
            
            tables["new"] = "N" # Do not recreate
            if chunkSize < 0:
                break;  # Done with the single load
            chunkStart, chunkEnd = self.UpdateChunkStartEnd(chunkEnd, chunkSize, maxValue)
        
        # Load only the data that we processed into Redshift.  We cannot use the run ETL date parition value
        # since we are loading the data based on record IDs
        customWhereCondition = tables["incrementalconditions"]["keyfield"] + " >= " + str(chunkStartData)
        self.LoadDataFromAthenaIntoRedShiftLocalScripts(tables, customWhereCondition)
        
    def ProcessTables(self, dbCommon, tables):
        '''
        Process the current table to load it up
        '''
        try:
            self.logger.debug(self.moduleName + " -- ProcessTables for  " + tables["table"] + " starting")
            self.fileUtilities.EmptyFolderContents(self.fileUtilities.sqlFolder)
            if "incrementalconditions" in tables:
                self.IncrementalLoad(dbCommon, tables)
            else:
                self.FlushAndFillUsingJDBC(dbCommon, tables)
            self.logger.debug(self.moduleName + " -- ProcessTables for  " + tables["table"] + " finished")
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessDatabase for " + tables["table"])
            raise

    def Start(self, logger, moduleName, filelocs):
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)
