'''
Created on Nov 1, 2017

@author: VIU53188
@license: IHS - not to be used outside the company
@summary: Modified from HistoricalBrent to use Athena Spark
'''

import os
import sys
import shutil

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.ExcelUtilities import ExcelUtilities
from AACloudTools.SparkUtilities import SparkUtilities

from Applications.Common.ApplicationBase import ApplicationBase

class HistoricalBrentAthenaSpark(ApplicationBase):
    '''
    Code to process Historical Brent file from network share
    '''

    def __init__(self):
        '''
        Initial settings
        '''
        super(HistoricalBrentAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))

    def DownloadFile(self, rawFolder):
        '''
        Download the file into the local folder.
        '''
        self.logger.debug(self.moduleName + " -- " + "DownloadFile starting ")
        fileNameDest = None
        flist = []
        try:
            rawFolder = self.localTempDirectory + '/raw/'
            for fnl in self.fileUtilities.ScanFolder(self.job["srcSharedFolder"]):
                
                fileNameDest = os.path.join(rawFolder + fnl)                
                self.logger.info(self.moduleName + " - Copying file from source folder...")
                shutil.copyfile(os.path.join(self.job["srcSharedFolder"], fnl), fileNameDest)
                fnamesplitarray = fnl.rsplit('.', 1)
                fExt = fnamesplitarray[1]
                if fExt == 'zip':
                    fnameLocation = rawFolder + fnl
                    self.fileUtilities.UnzipUsing7z(fnameLocation, rawFolder)
                    self.fileUtilities.DeleteFile(fnameLocation)
                    flist = self.fileUtilities.ScanFolder(rawFolder)
        except:
            self.logger.exception(self.moduleName + "- we had an error in getxlFilename ")
            raise 
        finally:
            self.logger.debug(self.moduleName + " -- " + "DownloadFile finished ")
        return flist

    def ProcessTables(self, dbCommon, tables):
        '''
        process steps:
        pulls file from share and place in raw folder
        '''
        try:
            rawFolder = self.localTempDirectory + '/raw/'
            flist = self.DownloadFile(rawFolder)
            xl = ExcelUtilities(self.logger)

            outPutFileName = self.fileUtilities.csvFolder + self.moduleName + '.csv'
            for fl in flist:
                xl.Excel2CSV(rawFolder + fl,\
                            'Sheet1',\
                            outPutFileName,\
                            self.fileUtilities.csvFolder)
                
            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            df = SparkUtilities.ReadCSVFile(spark, tables, self.job["delimiter"], False,
                                            self.fileUtilities.csvFolder, self.logger)
            if "adjustFormat" in tables:
                for fld in tables["adjustFormat"]:
                    df = SparkUtilities.FormatColumn(df, fld["name"], fld["inputFormat"])
            SparkUtilities.SaveParquet(df, self.fileUtilities)
            self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet)
            self.fileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder)
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