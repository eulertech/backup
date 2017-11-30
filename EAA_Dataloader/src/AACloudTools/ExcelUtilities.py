'''
Created on Jan 19, 2017

@author: Thomas Coffey
@license: IHS - not to be used outside the company
@note:  Utilities for Excel
@summary: set of Excel Utilities in a common library
        The purpose of this package is to run a full set of loads based on a jSON configuration file
@change: TCCF -> 1/25/2017  -> Initial creation
@change: TCC -> 4/26/2017 -> modified to conform to tslint standards
'''

import re
import csv
import xlrd
import sys

class ExcelUtilities(object):
    '''
    just a holding place to show start of class
    '''

    def __init__(self, logger):
        '''
        Constructor
        '''
        self.logger = logger
        self.instream = ""
        self.filename = ""
        self.outputdatadir = ""
        self.gwb = None

    def OpenStream(self, instream):
        '''
        takes the input stream and returns a workbook object
        '''
        from xlrd import open_workbook
        try:
            wb = open_workbook(file_contents=instream)
            self.gwb = wb
            return wb
        except:
            self.logger.exception("we had an error in ExcelUtilies in OpenStream")
            raise

    def OpenFile(self, infile):
        '''
         opens an actua file and returns a workbook object
        '''
        try:
            wb = xlrd.open_workbook(infile)
            self.gwb = wb
            return wb
        except:
            self.logger.exception("we had an error in ExcelUtilies in OpenFile")
            raise

    @staticmethod
    def ValidateOutputFolder(infolder):
        '''
        if the folder does not exists then create it
        '''
        import os
        try:
            os.makedirs(infolder)
        except OSError:
            pass
    def ConvertWBtoCSV(self, sh, outfile, indelm, defDateFormat='%m/%d/%Y', skiprows=0, omitBottomRows=0, cols=[]):
        '''
        Converts a specific workbooks sheet to a CSV file
        '''
        try:
            
            if sys.version[0] == '3':
                with open(outfile, 'w', newline = '') as fl:
                    cObj = csv.writer(fl, delimiter=indelm, quoting=csv.QUOTE_ALL)
                    self.logger.info('sheetname: %s, Rows: %s, Cols: %s' % (sh.name, sh.nrows, sh.ncols))
                    for rowNdx in range(0, sh.nrows-omitBottomRows):
                        if skiprows <= rowNdx:
                            row = []
                            for colNdx, cell in enumerate(sh.row(rowNdx)):
                                row.append(self.AdjustCellValue(cell, rowNdx, colNdx, defDateFormat))
                            cObj.writerow(row)
            else:
                with open(outfile, 'wb') as fl:
                    cObj = csv.writer(fl, delimiter=indelm, quoting=csv.QUOTE_ALL)
                    self.logger.info('sheetname: %s, Rows: %s, Cols: %s' % (sh.name, sh.nrows, sh.ncols))
                    for rowNdx in range(0, sh.nrows-omitBottomRows):
                        if skiprows <= rowNdx:
                            row = []
                            for colNdx, cell in enumerate(sh.row(rowNdx)):
                                if len(cols) > 0:
                                    if colNdx in cols:
                                        row.append(self.AdjustCellValue(cell, rowNdx, colNdx, defDateFormat))
                                else:
                                    row.append(self.AdjustCellValue(cell, rowNdx, colNdx, defDateFormat))
                            cObj.writerow(row)
        except:
            self.logger.exception("we had an error in ExcelUtilies in ConvertWBtoCSV")
            raise

    def ProcessStream(self, infile, instream, infolder, inSheetName, delm=',', defDateFormat='%m/%d/%Y'): #pylint: disable-msg=too-many-arguments
        '''
          parameters:
            infile  --> actual file name
            instream --> that actual stream , from a request object
            infolder --> where do you want the output sent
            inSheetName --> the actual sheet you want to pull from
        '''
        try:
            ###
            #  first we need to make sure that we have the folder if needed
            ###
            self.ValidateOutputFolder(infolder)
            wb = self.OpenStream(instream)
            sh = wb.sheet_by_name(inSheetName)
            ##
            # get the current file extension by splitting the current file name and looking
            # for the current extension value by splitting the current file name
            ##
            fnamesplitarray = infile.rsplit('.', 1)
            newfilename = infile.replace(fnamesplitarray[1], 'csv', 1)
            outfile = infolder + '/' + newfilename

            self.ConvertWBtoCSV(sh, outfile, delm, defDateFormat)
        except:
            self.logger.exception("we had an error in ExcelHelper in ProcessStream")
            raise

    # Load the AWS configuration for RedShift and S3
    def Excel2CSV(self, excelFile, sheetName, csvFile, folder, delm=',', defDateFormat='%m/%d/%Y', skiprows=0, omitBottomRows=0, sheetNo=0, cols=[]): #pylint: disable-msg=too-many-arguments
        '''
          parameters:
            ExcelFile  --> actual file name
            SheetName --> the actual sheet you want to pull from
            CSVFile  -->  name to save the output to
            infolder  --> the folder where to save the file to
            delm    -->  delimiter to use.  Defaults to ',' but can be overriden
        '''
        try:
            self.ValidateOutputFolder(folder)
            wb = self.OpenFile(excelFile)
            sh = None
            try:
                sh = wb.sheet_by_name(sheetName)
            except:
                sh = wb.sheet_by_index(sheetNo)
            self.ConvertWBtoCSV(sh, csvFile, delm, defDateFormat, skiprows, omitBottomRows, cols)
        except:
            self.logger.exception("we had an error in ExcelHelper in Excel2CSV")
            raise

    # Temp code that works fine with Python 36
    def Excel2CSVPython3(self, excelFile, sheetName, csvFile):
        '''
        method that is python3 compliant to convert a specific sheet to a csv file
        '''
        try:
            workbook = xlrd.open_workbook(excelFile)
            sh = workbook.sheet_by_name(sheetName)
            csvfile = open(csvFile, 'w')
            csvWriter = csv.writer(csvfile, quoting=csv.QUOTE_ALL)

            for rowidx in range(0, sh.nrows):
                row = [int(cell.value) if isinstance(cell.value, float) else cell.value for cell in sh.row(rowidx)]
                csvWriter.writerow(row)

            csvfile.close()
        except:
            self.logger.exception("we had an error in ExcelHelper during Excel2CSVPython3")
            raise

    class SheetLocation(object):   # pylint: disable=too-few-public-methods
        '''
        class to hold properties
        '''
        row = 0
        col = 0
        found = False
        msg = None

    def ExcelFindString(self, sht, inString, startrow=0, startcol=0, endrow=None, endcol=None): #pylint: disable-msg=too-many-arguments
        '''
        Finds a specific string in a range on a specific sheet
        '''
        try:
            stRow = startrow
            stCol = startcol
            endRow = endrow
            endCol = endcol
            if endrow is None:
                endRow = sht.nrows
            if endcol is None:
                endCol = sht.ncols

            loc = self.SheetLocation()
            loc.found = False
            for rowidx in range(stRow, endRow):
                for colNdx, cell in enumerate(sht.row(rowidx)):
                    if colNdx > endCol:
                        break
                    if colNdx >= stCol:
                        if inString in str(cell.value):
                            loc.row = rowidx
                            loc.col = colNdx
                            loc.found = True
                            return loc
            loc.msg = '%s was not found in sheet %s' % (inString, sht.name)
            return loc
        except:
            self.logger.exception("we had an error in ExcelHelper during ExcelFindString")
            raise
    def AdjustCellValue(self, inCell, inRow, inCol, defDateFormat='%m/%d/%Y'):
        '''
        make sure that the vlue we get from the cell is in a format that we can use
        '''
        import datetime
        retVal = {}
        try:
            if inCell.ctype == xlrd.XL_CELL_DATE:
                dttuple = xlrd.xldate_as_tuple(inCell.value, self.gwb.datemode)
                dObj = datetime.datetime(
                    dttuple[0], dttuple[1], dttuple[2],
                    dttuple[3], dttuple[4], dttuple[5]
                )
                outdate = dObj.strftime(defDateFormat)
                retVal = outdate
            elif inCell.ctype == xlrd.XL_CELL_NUMBER:
                if inCell.value.is_integer():
                    retVal = int(inCell.value)
                else:
                    retVal = inCell.value
            else:
                if sys.version[0] == '3':  # for python3
                    tpValue = inCell.value
                else:
                    tpValue = unicode(inCell.value).encode("utf-8")
                tpValue = re.sub(r'\n', ' ', tpValue)
                retVal = tpValue
            return retVal
        except:
            self.logger.exception("we had an error in ExcelHelper during AdjustCellValue at location %s, %s", (inRow, inCol))
            raise

    def ExcelFillRowToArray(self, sht, row=0, startcol=0, endcol=None, defDateFormat='%m/%d/%Y'): # pylint: disable=too-many-arguments
        '''
        returns an array of values that exists in the row
        '''
        try:
            stRow = row
            stCol = startcol
            endRow = row + 1
            endCol = endcol
            if endcol is None:
                endCol = sht.ncols
            retArray = []
            for rowNdx in range(stRow, endRow):
                for colNdx, cell in enumerate(sht.row(rowNdx)):
                    if colNdx <= endCol and colNdx >= stCol:
                        retArray.append(self.AdjustCellValue(cell, rowNdx, colNdx, defDateFormat))
                    elif colNdx > endCol:
                        break
            return retArray
        except:
            self.logger.exception("we had an error in ExcelHelper during ExcelFillRowToArray")
            raise

    def ExcelFillColToArray(self, sht, col=0, startrow=0, endrow=None, defDateFormat='%m/%d/%Y'): # pylint: disable=too-many-arguments
        '''
        returns an array of values that exists in a column
        '''
        try:
            stRow = startrow
            stCol = col
            endRow = endrow
            endCol = col
            if endrow is None:
                endRow = sht.nrows
            retArray = []
            for rowidx in range(stRow, endRow):
                for colNdx, cell in enumerate(sht.row(rowidx)):
                    if colNdx <= endCol and colNdx >= stCol:
                        retArray.append(self.AdjustCellValue(cell, rowidx, colNdx, defDateFormat))
                    elif colNdx > endCol:
                        break

            return retArray
        except:
            self.logger.exception("we had an error in ExcelHelper during ExcelFillRowToArray")
            raise

    def GetSheet(self, localFilepath, xl, shName):
        '''
        returns a reference to the sheet we are looking for
        '''
        sh = None
        try:
#  use the Excel helper utility to access the file and its contents
###
            wb = xl.OpenFile(localFilepath)

            sh = wb.sheet_by_name(shName)
        except:
            self.logger.exception("we had an error in : GetSheet for " + shName + " in " + localFilepath)
            raise
        return sh

    def FindRelevantRows(self, sht, col=0, startrow=0, endrow=None):# pylint: disable=too-many-arguments
        '''
        Returns an array of values in a row
        '''
        retArray = []
        try:
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
        except:
            self.logger.exception("we had an error in ExcelHelper during FindRelevantRows")
            raise
        return retArray
