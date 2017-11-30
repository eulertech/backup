'''
Main script to process the EIA data
Author - Christopher Lewis
License: IHS - not to be used outside the company
'''

import os
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class EIAAthenaSpark(ApplicationBase):
    '''
    Code to process the PGCR Air Markets data
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(EIAAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))

    @staticmethod
    def ProcessDataRecords(row):
        if row.series_id and row.data: # Get records that have valid series_id and data
            for items in row.data:
                # Yield will return each row (Return will return just 1 row)
                yield (row.series_id, items[0], items[1])

    def ProcessCatalogs(self, dbCommon, catalog):
        '''
        Process the current table to load it up
        '''
        try:
            FileUtilities.EmptyFolderContents(self.fileUtilities.gzipFolder) # Clear the folder from the previous run
            FileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder)   # Clear the folder from the previous run
            url = dbCommon["urlPrefix"] + catalog["name"] + "." + dbCommon["urlExt"]
            self.logger.info(self.moduleName + " - Processing url: " + url)

            localZipFilepath = self.fileUtilities.gzipFolder + "/" + \
                catalog["name"] + "." + dbCommon["urlExt"]

            self.fileUtilities.DownloadFromURL(url, localZipFilepath)

            self.fileUtilities.UnzipFile(localZipFilepath, self.fileUtilities.csvFolder)
            localFilepath = self.fileUtilities.csvFolder + "/" + catalog["name"] + ".txt"
            
            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            dfMaster = spark.read.json(localFilepath)
            dfMaster = dfMaster.filter(dfMaster.series_id != "")
            for table in catalog["tables"]:
                
                self.logger.info(self.moduleName + " -- " + "Processing table: " + table["table"])
                # The column names being used in the source may be different from the once in the final
                # database.  Select columns based on source and then rename to destination
                schemaSrc = SparkUtilities.BuildSparkSchema(table, useValidation=True)
                if table["dataSet"] == "attributes":
                    df = dfMaster.select(schemaSrc.names)
                elif table["dataSet"] == "data":
                    print(dfMaster.rdd.take(5)) # There is some instability we need to monitor.  Print seems to slow down and stabilize the run???
                    df = dfMaster.rdd.flatMap(lambda row: EIAAthenaSpark.ProcessDataRecords(row)).toDF(schemaSrc.names)
                else:
                    raise ValueError("Undefined dataSet type") 
                
                schemaDst = SparkUtilities.BuildSparkSchema(table)
                df = SparkUtilities.RenameColumnsToSchema(df, schemaDst)
                df = SparkUtilities.ConvertTypesToSchema(df, schemaDst)
                self.logger.info(self.moduleName + " -- " + "Done reading " + str(df.count()) + " rows.  Now saving as parquet file...")

                FileUtilities.EmptyFolderContents(self.fileUtilities.sqlFolder) # Clear the folder from the previous run
                SparkUtilities.SaveParquet(df, self.fileUtilities)
                self.UploadFilesCreateAthenaTablesAndSqlScripts(table, self.fileUtilities.parquet)
                self.LoadDataFromAthenaIntoRedShiftLocalScripts(table)
            
            self.logger.debug(self.moduleName + " -- " + "ProcessS3File for: " + url + " finished.\n\n")
        except:
            self.logger.exception("we had an error in EIA on ProcessS3File")
            raise

    def Start(self, logger, moduleName, filelocs):
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)
