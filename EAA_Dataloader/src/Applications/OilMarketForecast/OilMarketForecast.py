'''
Created on Sept 25, 2017

@author: VIU53188
@summary: This application will load the most current excel spreadsheet and load data from two of the sheets
          into redshift after creating a csv and storing in s3
          because the name can change we are looking for the latest file based on Date Modified
'''

import os
import glob
import datetime
import sys
import csv
from dateutil.parser import parse

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities
from AACloudTools.ExcelUtilities import ExcelUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class OilMarketForecast(ApplicationBase):
    '''
    classdocs
    '''
    def __init__(self):
        '''
        Constructor
        '''
        super(OilMarketForecast, self).__init__()
        self.fromDate = None
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def LoadData(self, tblJson):
        '''
        load the data from s3 into RedShift
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "LoadData" + " starting ")
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)
            s3folder = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"] + "/" + self.fromDate

            fldDelimiter = self.job["delimiter"]
            if "delimiter" in tblJson:
                fldDelimiter = tblJson["delimiter"]

            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": tblJson["schemaName"],
                                                 "tableName": tblJson["table"],
                                                 "s3Filename": s3folder,
                                                 "fileFormat": self.job["fileFormat"],
                                                 "dateFormat": self.job["dateFormat"],
                                                 "delimiter": fldDelimiter
                                             },
                                             self.logger, "N")
            self.logger.info(self.moduleName + " - Finished loading s3 data to " +\
                             tblJson["schemaName"] + '.' +  tblJson["table"])
            rsConnect.close()
            self.logger.debug(self.moduleName + " -- " + "LoadData" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in LoadData")
            raise

    def GetFromDate(self):
        '''
        using the currDate passed in we need to calculate the next date to use
        '''
        try:
            currDate = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=1), '%Y-%m-%d')
            toDate = datetime.datetime.strptime(currDate, '%Y-%m-%d')
            retDate = toDate.strftime('%Y-%m-%d')
            return retDate
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetFromDate")
            raise

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

    def GetMostRecentFile(self, folderPath):
        '''
        returns the name of the latest file in a folder
        '''
        retVal = None
        try:
            self.logger.debug(self.moduleName + " -- " + "scan folder " + folderPath + " starting ")
            retVal = max(glob.glob(folderPath + '/*'), key=os.path.getctime)
            self.logger.debug(self.moduleName + " -- " + "scan folder " + folderPath + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in : GetMostRecentFile using folder " + folderPath)
            raise
        return retVal

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

    @staticmethod
    def IsDate(string):
        '''
        just checks to see if the input string is a date or not
        '''
        try:
            parse(string)
            return True
        except ValueError:
            return False

    def FillDatesArray(self, xl, sh, rndx):
        '''
        take a row and filter the values so that we only have a the date and associated column
        '''
        retValArray = []
        try:
            datesArray = xl.ExcelFillRowToArray(sh, rndx)
            for cvNdx, cv in enumerate(datesArray):
                if self.IsDate(cv):
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

    def BulkUploadToS3(self):
        '''
        Uploads all GZIP files created into S3 to be uploaded later...
        '''
        self.logger.info(self.moduleName + " - Uploading GZIP files to s3 folder..." + self.fromDate)
        s3Location = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"] + "/" + self.fromDate
        S3Utilities.CopyItemsAWSCli(self.fileUtilities.gzipFolder,
                                    s3Location,
                                    "--recursive --quiet")

    def Start(self, logger, moduleName, filelocs):
        '''
        main routine
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
###
#  set up to run create folder
###
            self.fileUtilities.moduleName = self.moduleName
            self.fileUtilities.localBaseDirectory = self.localTempDirectory
            self.fileUtilities.CreateFolders(self.job["folders"])
            self.fromDate = self.GetFromDate()
###
            localFilepath = self.GetMostRecentFile(self.job["foldertoscan"])
#            localFilepath = r'C:\tmp\IHS Markit Outlook for Global Oil Market Fundamentals - September 2017.xlsx'
            for tables in self.job["tables"]:
                fname = self.fileUtilities.CreateTableSql(tables, self.fileUtilities.sqlFolder)
                RedshiftUtilities.PSqlExecute(fname, self.logger)
                outPutFileName = self.fileUtilities.csvFolder +\
                                 self.fromDate +\
                                 "_" + tables["table"]  + '.csv'
                outputGZ = self.fileUtilities.gzipFolder + self.fromDate +\
                           "_" + tables["table"]  + '.csv.gz'
                tableJson = tables
            xl = ExcelUtilities(logger)
            if sys.version[0] == '3':
                csvfile = open(outPutFileName, 'w', newline = '')
            elif sys.version[0] == '2':
                csvfile = open(outPutFileName, 'wb')
            csvWriter = csv.writer(csvfile, quoting=csv.QUOTE_ALL)

            if localFilepath is not None:
                self.ProcessFile(xl, localFilepath, csvWriter)

            csvfile.close()

            self.fileUtilities.GzipFile(outPutFileName, outputGZ)
            self.BulkUploadToS3()
            self.LoadData(tableJson)

            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)

            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"],\
                                             currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
