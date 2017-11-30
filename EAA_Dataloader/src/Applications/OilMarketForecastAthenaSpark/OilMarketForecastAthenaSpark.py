'''
Created on Sept 25, 2017

@author: VIU53188
@summary: This application will load the most current excel spreadsheet and load data from two of the sheets
          into redshift after creating a csv and storing in s3
          because the name can change we are looking for the latest file based on Date Modified
'''

import os
import sys
import csv

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from AACloudTools.ExcelUtilities import ExcelUtilities
from AACloudTools.DatetimeUtilities import DatetimeUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class OilMarketForecastAthenaSpark(ApplicationBase):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        super(OilMarketForecastAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    class CellDataClass(object):
        '''
        custom class to hold dates in spreadsheet contains a getter and setter concept
        '''
        def __init__(self):
            self.ndx = None
            self.val = None
        def SetValues(self, ndx, val):
            '''
            just sets the values
            '''
            self.ndx = ndx
            self.val = val
        def GetValues(self):
            '''
            returns the values
            '''
            return self.ndx, self.val

    def FillNamesArray(self, xl, sh, cndx, stRndx, eRndx): # pylint: disable=too-many-arguments
        '''
        take a row and filter the values so that we only have a the date and associated column
        '''
        retValArray = []
        try:
            namesArray = xl.ExcelFillColToArray(sh, cndx, stRndx, eRndx)
            for cvNdx, cv in enumerate(namesArray):
                lncv = len(cv)
                if lncv > 0:
                    dObj = self.CellDataClass()
                    dObj.val = cv
                    dObj.ndx = cvNdx
                    retValArray.append(dObj)
        except:
            self.logger.exception(self.moduleName + " - we had an error in : FillDatesArray ")
            raise
        return retValArray

    def FillDatesArray(self, xl, sh, rndx):
        '''
        take a row and filter the values so that we only have a the date and associated column
        '''
        retValArray = []
        try:
            datesArray = xl.ExcelFillRowToArray(sh, rndx)
            for cvNdx, cv in enumerate(datesArray):
                if DatetimeUtilities.IsDate(cv):
                    dObj = self.CellDataClass()
                    dObj.val = cv
                    dObj.ndx = cvNdx
                    retValArray.append(dObj)
        except:
            self.logger.exception(self.moduleName + " - we had an error in : FillDatesArray ")
            raise
        return retValArray

    def FillValuesArray(self, xl, sh, rndx, cndx):
        '''
        take a row and filter the values so that we only have a the date and associated column
        '''
        retValArray = []
        try:
            valsArray = xl.ExcelFillRowToArray(sh, rndx, cndx)
            for cvNdx, cv in enumerate(valsArray):
                dObj = self.CellDataClass()
                dObj.val = cv
                dObj.ndx = cvNdx
                retValArray.append(dObj)
        except:
            self.logger.exception(self.moduleName + " - we had an error in : FillDatesArray ")
            raise
        return retValArray

    def ProcessFile(self, xl, xlFileName, csvWriter): # pylint: disable=too-many-locals
        '''
        process the latest file
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "process file " + xlFileName + " starting ")
            for sheet in self.job["sheets"]:
                sh = xl.GetSheet(xlFileName, xl, sheet["name"])
                datesArray = self.FillDatesArray(xl, sh, sheet["headingline"])
                addLine = 0
                if "specificlable" in sheet:
                    sLoc = xl.ExcelFindString(sh, sheet["specificlable"], startcol=1)
                    cndx = sLoc.col
                    stRndx = sLoc.row
                    eRndx = sLoc.row + 1
                    addLine = sheet["dataline"]
                else:
                    cndx = 1
                    stRndx = sheet["headingline"] + 1
                    eRndx = None
                namesArray = self.FillNamesArray(xl, sh, cndx, stRndx, eRndx)
                valStartObj = datesArray[0]
                for nObj in namesArray:
                    valsArray = self.FillValuesArray(xl, sh, nObj.ndx + stRndx + addLine, valStartObj.ndx)
                    for dObject in datesArray:
                        outRecArray = []
                        outRecArray.append(nObj.val)
                        outRecArray.append(sheet["name"])
                        outRecArray.append(dObject.val)
                        tval = valsArray[dObject.ndx - valStartObj.ndx].val
                        outRecArray.append(tval)
                        lenTval = len(str(tval))
                        if lenTval > 0:
                            csvWriter.writerow(outRecArray)

            self.logger.debug(self.moduleName + " -- " + "process file " + xlFileName + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in : ProcessFile")
            raise

    def CreateCsvFile(self, tables):
        '''
        Create a reference to the CSV file
        '''
        try:
            outPutFileName = self.fileUtilities.csvFolder + tables["table"]  + '.csv'
            
            if sys.version[0] == '3':
                csvFile = open(outPutFileName, 'w', newline = '')
            elif sys.version[0] == '2':
                csvFile = open(outPutFileName, 'wb')
        except:
            self.logger.exception(self.moduleName + " - we had an error in : CreateCsvFile" )
            raise
        return csvFile

    def ProcessTables(self, dbCommon, tables):
        '''
        pulls data from different sheets and put that information into csv file
        '''
        try:
            xl = ExcelUtilities(self.logger)
            localFilepath = self.fileUtilities.FindMostCurrentFile(self.job["foldertoscan"])
            csvfile = self.CreateCsvFile(tables)
            csvWriter = csv.writer(csvfile, quoting=csv.QUOTE_ALL)

            if localFilepath is not None:
                self.ProcessFile(xl, localFilepath, csvWriter)

            csvfile.close()
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
