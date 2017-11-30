'''
Created on Oct 30, 2017

@author: VIU53188
@summary: pulls data from GTA database and puts into S3 modified from GTA module and converted to use Athena and Spark
'''

import os
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from AACloudTools.AthenaUtilities import AthenaUtilities
from AACloudTools.BCPUtilities import BCPUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class GTAAthenaSpark(ApplicationBase):
    '''
    Class used to pull GTA data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(GTAAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))


    def GetPartitionValue(self, tables, year):
        '''
        Get Partition Value based on year
        '''
        partitionValue = None
        if "incrementalconditions" in tables:
            # Partition date is the date the ETL is run on
            partitionValue = str(year)
        return partitionValue


    def UpdateChunkStartEnd(self, chunkEnd, chunkSize, maxValue):
        '''
        For incremental load update the start and end range
        '''
        chunkStart = chunkEnd + 1
        chunkEnd = chunkEnd + chunkSize
        if chunkEnd > maxValue:
            chunkEnd = maxValue
        return chunkStart, chunkEnd

    def ProcessTables(self, dbCommon, tables):
        '''
        Process the current table to load it up
        '''
        try:
            self.logger.debug(self.moduleName + " -- ProcessTables for  " + tables["table"] + " starting")
            
            # Cleanup first (TODO - Need a more generic way to do this)
            self.fileUtilities.EmptyFolderContents(self.fileUtilities.sqlFolder)
            
            # Variables used for handling chunks.  -1 for full load
            chunkStart =  chunkEnd = maxValue = chunkSize = -1
            
            if "incrementalconditions" in tables:
                incrementalConditions = tables["incrementalconditions"]
                if "startID" in incrementalConditions:
                    chunkEnd = incrementalConditions["startID"] - 1
                else:
                    athenaSchemaName = AthenaUtilities.ComposeAthenaSchemaName(tables["schemaName"])
                    try:
                        # This is where we last ended.  Start at 1 + this end
                        chunkEnd = int(AthenaUtilities.GetMaxValue(self.awsParams, athenaSchemaName, tables["table"], tables["distkey"], self.logger))
                    except ValueError:
                        chunkEnd = 0 # Table does not exist yet
                    except:
                        raise

                if "endID" in incrementalConditions:
                    maxValue = incrementalConditions["endID"]
                else:
                    # TODO - Fix this.  Also, we should start at the source min value not 0.
                    maxValue = 2000000000 #BCPUtilities.GetMaxValueSQLServer(dbCommon, tables, chunkStart)
                    
                chunkSize = tables["incrementalconditions"]["chunksize"]
                chunkStart, chunkEnd = self.UpdateChunkStartEnd(chunkEnd, chunkSize, maxValue)
                    
            fieldDelimiter = self.job["delimiter"]
            if "delimiter" in tables:
                fieldDelimiter = tables["delimiter"]
            
            while chunkStart <= maxValue:
                partitionValue = self.GetPartitionValue(tables, chunkStart)
                sqlPullDataScript = BCPUtilities.CreatePullScript(dbCommon, tables, chunkStart, chunkEnd,
                                                                  self.logger, self.fileUtilities, self.location)
                # Construct a file name that is meaning full.  That is, it has the start and end IDs
                outputCSV = self.fileUtilities.csvFolder + BCPUtilities.ComponseRangeString(chunkStart, chunkEnd) + ".csv"
                self.fileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder)
                self.bcpUtilities.BulkExtract(sqlPullDataScript, outputCSV, dbCommon, tables, fieldDelimiter,
                                              self.job["bcpUtilityDirOnLinux"], self.fileUtilities, self.logger)
                # Process the data using Spark and save as Parquet
                spark = SparkUtilities.GetCreateSparkSession(self.logger)
                schema = SparkUtilities.BuildSparkSchema(tables)
                df = (spark.read
                         .format("com.databricks.spark.csv")
                         .options(header='false', delimiter=fieldDelimiter)
                         .schema(schema)
                         .load(self.fileUtilities.csvFolder)
                         )
                df.printSchema()
                df.show()
                df = SparkUtilities.ProcessSpecialCharsIfAny(df, tables)
            
                self.logger.info(self.moduleName + " -- " + "DONE READING " + str(df.count()) + " ROWS.  Now saving as parquet file...")
                self.fileUtilities.EmptyFolderContents(self.fileUtilities.parquet)
                SparkUtilities.SaveParquet(df, self.fileUtilities)
            
                # Need to load the data and clear the local space
                self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet, partitionValue)
                
                tables["new"] = "N" # Do not recreate
                if chunkSize < 0:
                    break;  # Done with the single load
                chunkStart, chunkEnd = self.UpdateChunkStartEnd(chunkEnd, chunkSize, maxValue)
            
            # TODO - Need to make sure we don't end up with duplicate data if we run the code
            # Twice on the same day
            self.LoadDataFromAthenaIntoRedShiftLocalScripts(tables)

            self.logger.debug(self.moduleName + " -- ProcessTables for  " + tables["table"] + " finished")
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessDatabase for " + tables["table"])
            raise

    def Start(self, logger, moduleName, filelocs):
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)
