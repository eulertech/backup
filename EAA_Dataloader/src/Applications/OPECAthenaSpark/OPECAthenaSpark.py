'''
Created on Nov 11, 2017

@author: viu53188
@summary: Loads the OPEC Calendar data excel file pulled from the OPEC web site.
        modified from OPEC to use Athena and Spark framework
'''

import os
import shutil

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from AACloudTools.ExcelUtilities import ExcelUtilities


from Applications.Common.ApplicationBase import ApplicationBase

class OPECAthenaSpark(ApplicationBase):
    '''
    This class is used to control the data load process from different OPEC file sources.
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(OPECAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
        
    def DownloadFile(self, srcCategory):
        '''
        Download the file into the local folder.
        '''
        self.logger.debug(self.moduleName + " -- " + "DownloadFile for file " + srcCategory["fileName"] + " starting ")
        fileNameDest = None
        try:
            scanLen = len(self.fileUtilities.ScanFolder(srcCategory["srcFolder"], srcCategory["fileName"]))
            if scanLen > 0:
                fileNameDest = os.path.join(self.localTempDirectory, srcCategory["fileName"])
                self.logger.info(self.moduleName + " - Copying file from source folder...")
                shutil.copyfile(os.path.join(srcCategory["srcFolder"], srcCategory["fileName"]), fileNameDest)
        except:
            self.logger.exception(self.moduleName + "- we had an error in DownloadFile ")
            raise Exception(self.moduleName + " - " + srcCategory["unzipFilename"] + " file not found in source directory.")
        finally:
            self.logger.debug(self.moduleName + " -- " + "DownloadFile for file " + srcCategory["fileName"] + " starting ")
        return fileNameDest

    def CreateCSVFile(self, processingFile, srcCategory):
        '''
        Converts the actual excel file into csv for the worksheet configured.
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "GetCSVFile" + " starting ")

            xl = ExcelUtilities(self.logger)
            outPutFileName = self.fileUtilities.csvFolder + self.moduleName + '.csv'
            xl.Excel2CSV(processingFile,\
                        srcCategory["worksheetName"],\
                        outPutFileName,\
                        self.fileUtilities.csvFolder,\
                        skiprows=srcCategory["skipRows"],\
                        omitBottomRows=srcCategory["skipFooter"])            

            self.logger.debug(self.moduleName + " -- " + "GetCSVFile" + " finished ")
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to convert source file into csv..." + err.message)
            raise Exception(err.message)

    def ProcessTables(self, dbCommon, tables):
        '''
        process steps:
        pulls file from share and place in raw folder
        '''
        try:

            processingFile = self.DownloadFile(self.job["srcCategories"])
            self.CreateCSVFile(processingFile, self.job["srcCategories"])

            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            schemaAllString = SparkUtilities.BuildSparkSchema(tables, True)
            schema = SparkUtilities.BuildSparkSchema(tables)
            df = (spark.read
                    .format("com.databricks.spark.csv")
                    .options(header=False, delimiter=self.job["srcCategories"]["delimiter"])
                    .option("ignoreTrailingWhiteSpace", "true")
                    .option("ignoreLeadingWhiteSpace", "true")            
                    .schema(schemaAllString)
                    .load(self.fileUtilities.csvFolder)
                )
            df = SparkUtilities.ReplaceAll(df, "\xE2\x80\x93", "")
            df2 = SparkUtilities.ConvertTypesToSchema(df, schema)            
            SparkUtilities.SaveParquet(df2, self.fileUtilities)
            self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet)
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