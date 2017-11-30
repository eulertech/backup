'''
Created on Feb 9, 2017

@author: VIU53188
@summary: This application will pull RigCount data from Excel Spreadsheet and load it into RedShift
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
import ntpath
import csv

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.ExcelUtilities import ExcelUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class RigCount(ApplicationBase):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        super(RigCount, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    class BlockClass(object):
        '''
        structure to hold the block
        '''
        def __init__(self):
            self.logger = None
            self.moduleName = None
            self.xl = None
            self.sh = None
            self.startlabel = None
            self.totallable = None

        def SetMainValues(self, logger, moduleName, xl, sh):
            '''
            sets the primary values that rarely change
            '''
            self.logger = logger
            self.moduleName = moduleName
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

    def GetFilesFromS3(self):
        '''
        pull down files from s3
        '''
        localFilepath = None
        try:
            self.logger.debug(self.moduleName + " -- " + "GetFilesFromS3" + " starting ")
            s3Key = self.job["s3SrcDirectory"] + "/" + self.job["filetoscan"]
            self.logger.info(self.moduleName + " - Processing file: " + s3Key)
            localFilepath = self.localTempDirectory + "/" + ntpath.basename(s3Key)
            S3Utilities.DownloadFileFromS3(self.awsParams.s3, self.job["bucketName"], s3Key, localFilepath)
            self.logger.debug(self.moduleName + " -- " + "GetFilesFromS3" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in : GetFilesFromS3")
            raise
        return localFilepath

    def GetSheet(self, localFilepath, xl, shName):
        '''
        returns a reference to the sheet we are looking for
        '''
        sh = None
        try:
            self.logger.debug(self.moduleName + " -- " + "GetSheet for " + shName + " starting ")
#  use the Excel helper utility to access the file and its contents
###
            wb = xl.OpenFile(localFilepath)

            sh = wb.sheet_by_name(shName)
            self.logger.debug(self.moduleName + " -- " + "GetSheet for " + shName + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in : GetSheet")
            raise
        return sh

    def LoadData(self, outPutFileName):
        '''
        load data into Redshift
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "LoadData for " + outPutFileName + " starting ")
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

            RedshiftUtilities.LoadFileIntoRedshift(rsConnect,
                                                   self.awsParams.s3,
                                                   self.logger,
                                                   self.fileUtilities,
                                                   outPutFileName,
                                                   self.job["destinationSchema"],
                                                   self.job["tableName"],
                                                   self.job["fileFormat"],
                                                   self.job["dateFormat"],
                                                   self.job["delimiter"])
            rsConnect.close()

            self.logger.debug(self.moduleName + " -- " + "LoadData for " + outPutFileName + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in : LoadData")
            raise

    def Start(self, logger, moduleName, filelocs):
        '''
        main routine
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
            localFilepath = self.GetFilesFromS3()
###
#  look for files to process
###
            xl = ExcelUtilities(logger)
###
            sh = self.GetSheet(localFilepath, xl, "DataImport")
##
##
#  generate CSV file
##
            outPutFileName = self.localTempDirectory + '/RigCountdata.csv'
            csvfile = open(outPutFileName, 'wb')
            csvWriter = csv.writer(csvfile)
###
#  time to get data
###

            block = self.BlockClass()
            block.SetMainValues(logger, moduleName, xl, sh)

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
            self.LoadData(outPutFileName)

            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)

            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
