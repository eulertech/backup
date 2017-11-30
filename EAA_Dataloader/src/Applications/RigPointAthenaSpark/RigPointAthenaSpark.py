'''
Created on Nov 8, 2017

@author: viu53188
@summary: Loads Excel files coming from IHS PetroData RIGPOINT.
        Modified from RIGPOINT to use Athena and Spark framework
'''
import os

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.ExcelUtilities import ExcelUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class RigPointAthenaSpark(ApplicationBase):
    '''
    RIGPOINT utilization report loader
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(RigPointAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def CreateCSVFile(self, srcFileName, reportConfig):
        '''
        create csv files from excel files
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "CreateCSVFile" + srcFileName + " starting ")
            srcFile = self.localTempDirectory + '/raw/' + srcFileName
            destFile = self.fileUtilities.csvFolder + os.path.splitext(srcFileName)[0] + ".csv"
            xl = ExcelUtilities(self.logger)

            xl.Excel2CSV(srcFile,\
                        None,\
                        destFile,\
                        self.fileUtilities.csvFolder,\
                        defDateFormat='%Y-%m-%d',\
                        skiprows=reportConfig["skipRows"],\
                        omitBottomRows=reportConfig["skipFooter"],\
                        sheetNo=reportConfig["excelSheetNo"])            
            
            self.logger.debug(self.moduleName + " -- " + "CreateCSVFile" + srcFileName + " finished ")
           
        except Exception as err:
            self.logger.error(self.moduleName + " had an error in CreateCSVFile error - : " + err.message)
            raise
        
    def ProcessReports(self):
        '''
        Process all reports
        '''
        self.logger.debug(self.moduleName + " -- " + "ProcessReports" + " starting ")
        rawFolder = self.localTempDirectory + '/raw/'
        for report in self.job["reports"]:
            for fileName in self.fileUtilities.ScanFolder(rawFolder, None, report["fileInputExt"]):
                if report["fileInputPrefix"] in fileName:
                    self.CreateCSVFile(fileName, report)
        self.logger.debug(self.moduleName + " -- " + "ProcessReports" + " finished ")

    def DownloadFilesFromS3(self, tablesJson):
        '''
        Download all files from the s3 data folder.
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "DownloadFilesFromS3" + " starting ")
            rawFolder = self.localTempDirectory + '/raw/'
            S3Utilities.CopyItemsAWSCli("s3://" + tablesJson["srcBucketName"] + tablesJson["srcS3DataFolder"],
                                        rawFolder,
                                        "--recursive --quiet")

            self.logger.debug(self.moduleName + " -- " + "DownloadFilesFromS3" + " finished ")
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to download files from s3. Error: " + err.message)
            raise

    def ProcessTables(self, dbCommon, tables):
        '''
        process steps:
        pulls file from share and place in raw folder
        '''
        try:
            self.DownloadFilesFromS3(tables)
            self.ProcessReports()
            
            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            df = SparkUtilities.ReadCSVFile(spark, tables, tables["delimiter"], False,
                                            self.fileUtilities.csvFolder, self.logger)
            if "adjustFormat" in tables:
                for fld in tables["adjustFormat"]:
                    df = SparkUtilities.FormatColumn(df, fld["name"], fld["inputFormat"])
            SparkUtilities.SaveParquet(df, self.fileUtilities)
            self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet)
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