'''
Created on Mar 22, 2017

@author: Hector Hernandez
@summary: Loads the OPEC Calendar data excel file pulled from the OPEC web site.
@note: Restructured to use SQL Server and be pylint compliance: VIU53188
'''

import os
import datetime
import re

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from AACloudTools.AthenaUtilities import AthenaUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class ScenariosAthenaSpark(ApplicationBase):
    '''
    This class is used to control the data load process from different OPEC file sources.
    '''

    def __init__(self):
        '''
        Initial settings
        '''
        super(ScenariosAthenaSpark, self).__init__()

        self.cBlank = ''
        self.masterSchema = None
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def GetPartitionValue(self):
        '''
        For incremental table get the partition value is the run date.  Otherwise it is none
        '''
        # Partition date is the date the ETL is run on
        partitionValue = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=1), '%Y%m%d')
        return partitionValue
      
    def GetParameters(self, table):
        '''
        get the value of the last valuation date
        '''
        try:
            athenaSchemaName = AthenaUtilities.ComposeAthenaSchemaName(table["schemaName"])
            try:
                maxval = int(AthenaUtilities.GetMaxValue(self.awsParams, athenaSchemaName, table["paramTable"], "etl_rundate", self.logger))
                prevPartition = maxval
            except:
                prevPartition = 20000101  # this is just to make sure we have a period to start with and by default is Jan 2000                     
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetParameters")
            raise
        return str(prevPartition)

    def CreatePullScript(self, tables, lastDate):
        '''
        takes the template for the pull script and customizes it for the data we need
        '''
        sqlPullDataScript = None
        try:
            self.logger.debug(self.moduleName + " -- " + "CreatePullScript" + " starting ")
            if lastDate is not None:
                fromDate = datetime.datetime.strptime(lastDate, '%m/%d/%Y')
                if fromDate > datetime.datetime.today()-datetime.timedelta(days=1):
                    fromDate = datetime.date.today()-datetime.timedelta(days=1)
                fromDate = datetime.datetime.strftime(fromDate, '%m/%d/%Y')

            sqlPullDataTemplate = self.location + '/SQL/' + tables["pullTemplate"]
            sqlPullDataScript = self.fileUtilities.sqlFolder + re.sub('Template.sql$', '.sql', tables["pullTemplate"])
            FileUtilities.RemoveFileIfItExists(sqlPullDataScript)

            with open(sqlPullDataTemplate) as infile, open(sqlPullDataScript, 'w') as outfile:
                for line in infile:
                    line = line.replace('{lastrundate}', fromDate)
                    outfile.write(line)
            self.logger.debug(self.moduleName + " -- " + "CreatePullScript" + " finished ")
            if fromDate is not self.cBlank:
                fromDate = datetime.datetime.strptime(fromDate, '%m/%d/%Y')
                fromDate = fromDate.strftime('%Y%m%d')
        except:
            self.logger.exception(self.moduleName + " - we had an error in CreatePullScript")
            raise
        return sqlPullDataScript

    def ProcessTables(self, dbCommon, tables):
        '''
        get the last partition value and use that as the date to pull data
        then put that data into Athena
        '''
        try:
            outputCSV = self.fileUtilities.csvFolder + self.moduleName + ".CSV"
            fieldTerminator = self.job["fieldTerminator"]
            if "fieldTerminator" in tables:
                fieldTerminator = tables["fieldTerminator"]            
            rawFolder = self.localTempDirectory + "/raw/"
            rowTerminator = None # Not using this. Stick with the default of CR/LF.  self.job["rowTerminator"]
 
            if "pullTemplate" in tables:
                lastRunDate = self.GetParameters(tables)
                formattedLastRunDate = lastRunDate[4:6] + '/' + lastRunDate[6:8] + '/' + lastRunDate[:4]
                sqlPullDataScript = self.CreatePullScript(tables, formattedLastRunDate)
                self.bcpUtilities.BulkExtract(self.fileUtilities.LoadSQLQuery(sqlPullDataScript),
                                              outputCSV, dbCommon, tables, fieldTerminator, rowTerminator,
                                              self.job["bcpUtilityDirOnLinux"], self.fileUtilities, self.logger)
                
                self.masterSchema = SparkUtilities.BuildSparkSchema(tables)
                self.fileUtilities.MoveFilesFromOneFolderToAnother(self.fileUtilities.csvFolder,\
                                                                   rawFolder,\
                                                                   '*.csv')                
                return
###
#  load data frame from CSV file
###
            partitionValue = self.GetPartitionValue()
            self.fileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder)
            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            df = (spark.read
                    .format("com.databricks.spark.csv")
                    .options(header=False, delimiter=fieldTerminator)
                    .schema(self.masterSchema)
                    .load(rawFolder)
                )
            cols = []
            for field in tables["fields"]:
                if "athenaOnly" in field:
                    if field["athenaOnly"] != "Y":
                        cols.append(field["name"])
                else:
                    cols.append(field["name"])
            if tables["type"] == "attributes":
                dfAttributes = df.select(cols).distinct()
                if dfAttributes.count() == 0:
                    self.logger.debug(self.moduleName + " - no records to process for Attribute data" )
                    return
                SparkUtilities.SaveParquet(dfAttributes, self.fileUtilities)
            elif tables["type"] == "series":
                dfSeries = df.select(cols)  
                if "adjustFormat" in tables:
                    for fld in tables["adjustFormat"]:
                        dfSeries = SparkUtilities.FormatColumn(dfSeries, fld["name"], fld["inputFormat"])
                if dfSeries.count() == 0:
                    self.logger.debug(self.moduleName + " - no records to process for Series data" )
                    return
                SparkUtilities.SaveParquet(dfSeries, self.fileUtilities)
            
            self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet, partitionValue)
            if "loadToRedshift" in tables and tables["loadToRedshift"] == "Y":
                self.LoadDataFromAthenaIntoRedShiftLocalScripts(tables)
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessTables")
            raise   
     
    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs) 
