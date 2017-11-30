'''
Created on Sep 27, 2017

@author: Hector Hernandez
@summary: Loads the AutoInsight Scenario data.

'''

import os
import sys
import shutil
import calendar
import pandas

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Common.ApplicationBase import ApplicationBase
from xlrd import XLRDError

class AutoInsightAthenaSpark(ApplicationBase):
    '''
    This class is used to control the data load process for Auto Insight.
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(AutoInsightAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    @staticmethod
    def FormatColNameDate(dtText):
        '''
        Converts the abbreviated date to YYYY-MM-01 format
        '''
        textFixed = None
        if sys.version[0] == '2':
            customException = StandardError()
        elif sys.version[0] == '3':
            customException = Exception()
        try:
            textFixed = str(dtText[4:]) + "-" + str(list(calendar.month_abbr).index(dtText[:3])) + "-01"
        except customException:
            textFixed = dtText

        return textFixed

    def ProcessFiles(self, dbCommon):
        '''
        Controls the workflow for the conversion, clean up and pack of the input files.
        '''
        srcFileName = dbCommon["srcSharedFolder"] + dbCommon["fileName"]
        self.logger.info(self.moduleName + " - Processing file: " + srcFileName)
        
        dstFileName = self.fileUtilities.csvFolder + dbCommon["fileName"]
        shutil.copyfile(srcFileName, dstFileName)
        csvFilename = dstFileName + ".csv"
        
        try:
            columnNames = []
            df = pandas.read_excel(dstFileName, sheet_name=dbCommon["worksheetName"], index_col=None,
                                   na_values=None,skiprows=dbCommon["skipRows"], skip_footer=dbCommon["skipFooter"])
            for colName in df.head(0):
                if colName not in dbCommon["columns_no_melt"]:
                    columnNames.append(self.FormatColNameDate(colName))
                else:
                    columnNames.append(colName)
            df.columns = columnNames
            df = df.melt(id_vars=dbCommon["columns_no_melt"])
            df.to_csv(csvFilename, header=False, sep=str(dbCommon["delimiter"]), encoding='utf-8', index=False)
        except XLRDError:
            self.logger.info(self.moduleName + " - No tab named '" + dbCommon["worksheetName"] + "' in " + dstFileName)
        except Exception:
            self.logger.error(self.moduleName + " - Error while trying to process file " +  dstFileName)
            raise
        finally:
            FileUtilities.RemoveFileIfItExists(dstFileName)

    def ProcessTables(self, dbCommon, tables):
        '''
        Process the current table to load it up
        '''
        self.logger.debug(self.moduleName + " -- ProcessTables for " + tables["table"] + " starting")
        self.ProcessFiles(dbCommon)
        
        spark = SparkUtilities.GetCreateSparkSession(self.logger)
        df = SparkUtilities.ReadCSVFile(spark, tables, dbCommon["delimiter"], False,
                                        self.fileUtilities.csvFolder, self.logger)
        SparkUtilities.SaveParquet(df, self.fileUtilities)
        self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet)
        self.LoadDataFromAthenaIntoRedShiftS3Scripts(tables)
        self.logger.debug(self.moduleName + " -- ProcessTables for " + tables["table"] + " Done.")
    
    def Start(self, logger, moduleName, filelocs):
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)
