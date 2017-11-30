'''
Created on Nov 14, 2017

@author: VIU53188
@summary: Modified from RigCount to use the Athena Spark framework 
        This application will pull RigCount data from Excel Spreadsheet and load it into RedShift
        1) take in the instance of ApplicationBase that sets up all the standard configurations
        2) pull file down from S3
        3) open Excel file and look for Sheet "DataImport" normally the first sheet
        4) Look for value in col "B" = "DUC Wells"  This will tell you the start location
        5) Next line will contain all the dates so we need to put them in an array
        6) 2 lines down from start location is where the data actually starts in Col "C" for name
        7) Starting in column D until the end of the array from step 4 is where the values are kept
        8) Last line that we need to process is the "Total" line but in the sheet they are formulas so we may have to calculate these values
        9) Generate CSV from values  (might use pandas dataFrame for this?)
        10) Load data into RedShift

'''

import os
import csv

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.SparkUtilities import SparkUtilities
from AACloudTools.ExcelUtilities import ExcelUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class RigCountAthenaSpark(ApplicationBase):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        super(RigCountAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    class BlockClass(object):
        '''
        structure to hold the block
        '''
        def __init__(self):
            self.xl = None
            self.sh = None
            self.startlabel = None
            self.totallable = None

        def SetMainValues(self, logger, moduleName, xl, sh):
            '''
            sets the primary values that rarely change
            '''
            self.xl = xl
            self.sh = sh
        def SetSecondaryValues(self, startlabel, totallable):
            '''
            sets the secondary lables
            '''
            self.startlabel = startlabel
            self.totallable = totallable

    class BlockDataClass(object):# pylint: disable=too-few-public-methods
        '''
        structure to hold the block data
        '''
        datesarray = []
        resourcearray = []
        startcol = None

    class ResourcesClass(object):# pylint: disable=too-few-public-methods
        '''
        structure to hold the resources
        '''
        idx = None
        value = None

    def FindRelevantRows(self, sht, col=0, startrow=0, endrow=None):# pylint: disable=too-many-arguments
        '''
        Returns an array of values in a row
        '''
        import xlrd
        retArray = []
        try:
            self.logger.debug(self.moduleName + " -- " + "FindRelevantRows" + " starting ")
            stRow = startrow
            stCol = col
            enRow = endrow
            enCol = col
            if endrow is None:
                enRow = sht.nrows
            for rowNdx in range(stRow, enRow):
                for colNdx, cell in enumerate(sht.row(rowNdx)):
                    if colNdx <= enCol and colNdx >= stCol:
                        if cell.ctype != xlrd.XL_CELL_EMPTY:
                            retArray.append(rowNdx)
                    elif colNdx > enCol:
                        break

            self.logger.debug(self.moduleName + " -- " + "FindRelevantRows" + " finished ")
        except:
            self.logger.exception("we had an error in ExcelHelper during ExcelFillRowToArray")
            raise
        return retArray

    def ProcessBlock(self, block):
        '''
        Process a block of data
        '''
        data = self.BlockDataClass()
        try:
            self.logger.debug(self.moduleName + " -- " + "ProcessBlock" + " starting ")

            startLoc = block.xl.SheetLocation()
            startLoc = block.xl.ExcelFindString(block.sh, block.startlabel, startcol=1, endcol=1)

            totalLoc = block.xl.SheetLocation()
            totalLoc = block.xl.ExcelFindString(block.sh, block.totallable, startrow=startLoc.row+1, startcol=2, endcol=2)

            data.datesarray = block.xl.ExcelFillRowToArray(block.sh, row=startLoc.row+1, startcol=startLoc.col+2)
            rowsUsed = self.FindRelevantRows(block.sh, col=1, startrow=startLoc.row+2, endrow=totalLoc.row)
            data.resourcearray = []

            for row in rowsUsed:
                resRec = self.ResourcesClass()
                resRec.idx = row
                resRec.value = block.sh.cell_value(row, 2)
                data.resourcearray.append(resRec)

            data.startcol = startLoc.col
            self.logger.debug(self.moduleName + " -- " + "ProcessBlock" + " finished ")
        except:
            self.logger.exception("we had an error in RigCount during ProcessBlock")
            raise
        return data

    def OutPutToCSV(self, block, data, csvwriter):
        '''
        output data to a csv file
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "OutPutToCSV" + " starting ")
            outRecArray = []
            for row in data.resourcearray:
                dataRowArray = []
                dataRowArray = block.xl.ExcelFillRowToArray(block.sh, row=row.idx, startcol=data.startcol+2)
                for colNdx, dte in enumerate(data.datesarray):
                    if dte != "":
                        outRecArray = []
                        outRecArray.append(block.startlabel)
                        outRecArray.append(dte)
                        outRecArray.append(row.value)
                        outRecArray.append(dataRowArray[colNdx])
                        csvwriter.writerow(outRecArray)
            self.logger.debug(self.moduleName + " -- " + "OutPutToCSV" + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in OutPutToCSV ")
            raise

    def DownloadFilesFromS3(self, tablesJson):
        '''
        Download all files from the s3 data folder.
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "DownloadFileFromS3" + " starting " + tablesJson["srcFile"])
            rawFolder = self.localTempDirectory + '/raw/'
            S3Utilities.CopyItemsAWSCli("s3://" + tablesJson["srcBucketName"] + tablesJson["srcS3DataFolder"],
                                        rawFolder,
                                        "--recursive --quiet")

            self.logger.debug(self.moduleName + " -- " + "DownloadFileFromS3" + " finished " + tablesJson["srcFile"])
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to download file from s3. Error: " + err.message)
            raise
    
    def GenerateCSVFromSpreadSheet(self, tables, srcFolder, srcFileName):
        '''
        takes the spreadsheet and generates a csv file based on specific conditions
        '''
        try:
            xl = ExcelUtilities(self.logger)
            localFilepath = srcFolder + srcFileName
            sh = xl.GetSheet(localFilepath, xl, "DataImport")
            outPutFileName = self.fileUtilities.csvFolder + tables["table"] + '.csv'
            csvfile = open(outPutFileName, 'wb')
            csvWriter = csv.writer(csvfile)
            block = self.BlockClass()
            block.SetMainValues(self.logger, self.moduleName, xl, sh)

###
#  load the bData object that will contain all the data to be output
###
            block.SetSecondaryValues("Crude Oil Production (Bbl/d)", "Lwr-48")

            bData = self.BlockDataClass()
            bData = self.ProcessBlock(block)

            self.OutPutToCSV(block, bData, csvWriter)
###############################
            block.SetSecondaryValues("DUC Wells", "Total")

            bData = self.BlockDataClass()
            bData = self.ProcessBlock(block)

            self.OutPutToCSV(block, bData, csvWriter)
###############################
            block.SetSecondaryValues("New wells (No DUCs)", "Total")

            bData = self.BlockDataClass()
            bData = self.ProcessBlock(block)

            self.OutPutToCSV(block, bData, csvWriter)
###############################
            block.SetSecondaryValues("Total Producing Wells", "Total")

            bData = self.BlockDataClass()
            bData = self.ProcessBlock(block)

            self.OutPutToCSV(block, bData, csvWriter)
###############################
            block.SetSecondaryValues("Production per well", "")

            bData = self.BlockDataClass()
            bData = self.ProcessBlock(block)

            self.OutPutToCSV(block, bData, csvWriter)
###############################
            block.SetSecondaryValues("Active Rig Count", "Total")

            bData = self.BlockDataClass()
            bData = self.ProcessBlock(block)

            self.OutPutToCSV(block, bData, csvWriter)
####
#  this will be the end of the processing loop so we can close it
####
            csvfile.close()
            
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to download file from s3. Error: " + err.message)
            raise
        return outPutFileName
        
    def ProcessTables(self, dbCommon, tables):
        '''
        process steps:
        pulls file from share and place in raw folder
        '''
        try:
            self.DownloadFilesFromS3(tables)
            for fileName in self.fileUtilities.ScanFolder(self.localTempDirectory + '/raw/'):
                outPutFileName = self.GenerateCSVFromSpreadSheet(tables, self.localTempDirectory + '/raw/', fileName)

            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            df = SparkUtilities.ReadCSVFile(spark, tables, tables["delimiter"], False,
                                            self.fileUtilities.csvFolder, self.logger)
            if "adjustFormat" in tables:
                for fld in tables["adjustFormat"]:
                    df = SparkUtilities.FormatColumn(df, fld["name"], fld["inputFormat"])
            SparkUtilities.SaveParquet(df, self.fileUtilities)
            self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet)
            self.LoadDataFromAthenaIntoRedShiftLocalScripts(tables)
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessTables")
            raise

    def Start(self, logger, moduleName, filelocs):
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)
        
