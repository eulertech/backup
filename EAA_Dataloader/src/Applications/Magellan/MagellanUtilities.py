'''
Created on May 9, 2017

@author: VIU53188
'''
from __future__ import unicode_literals
import os
from os import listdir
from os.path import isfile, join
import ntpath
import csv
import re
from datetime import datetime
import simplejson as json
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.RedshiftUtilities import RedshiftUtilities

class MagellanUtilities(object):
    '''
    method that can be used to convert json files to csv files
    '''
    def __init__(self):
        self.commonParams = {}
        self.fl = None
        self.moduleName = 'MagellanUtilities'
        self.localTempDirectory = None
        self.fileUtilities = None
        self.logger = None

    def BuildTables(self, tables):
        '''
        Builds the tables
        '''
        try:
            for table in tables:
                fname = self.commonParams["sqlFolder"] + "Create_" + table["name"] + ".sql"
                RedshiftUtilities.PSqlExecute(fname, self.logger)
        except:
            self.logger.exception(self.moduleName + "- we had an error in BuildTables")
            raise

    def CreateSQLFiles(self, proc, dest):
        '''
        Routine to create sql files to use to create tables in RedShift
        '''
        try:
            for table in proc["tables"]:
                fname = self.commonParams["sqlFolder"] + "Create_" + table["name"] + ".sql"
                self.logger.info(fname)
                outfile = open(fname, "w")
                outLine = "DROP TABLE IF EXISTS {}.{};".format(dest, table["name"])
                outLine = FileUtilities.PutLine(outLine, outfile)
                outLine = "CREATE TABLE {}.{} (".format(dest, table["name"])
                outLine = FileUtilities.PutLine(outLine, outfile)
                ndx = 0
                for fld in table["fields"]:
                    if ndx > 0:
                        outLine = ','
                    ndx = ndx + 1
                    outLine = outLine + fld["name"] + " " + fld["type"]
                    if fld["type"] == "VARCHAR":
                        outLine = outLine + "(" + fld["size"] + ")  ENCODE LZO"
                    outLine = FileUtilities.PutLine(outLine, outfile)
                outfile.write(");")
                outfile.close()
        except:
            self.logger.exception(self.moduleName + "- we had an error in CreateSQLFiles ")
            raise

    def InitiateCsvFile(self, batchName, preFx, subFolder):
        '''
        initial creation of CSV file
        '''
        try:
            csvFileName = self.commonParams["csvFolder"] + subFolder + "/" + preFx + batchName + ".csv"
            outFile = open(csvFileName, "ab")
            return outFile
        except:
            self.logger.exception(self.moduleName + "- we had an error in InitiateCsvFile ")
            raise

    def CreateCsvFile(self, batchName, preFx, outputHolding, subFolder):
        '''
        create the csv file from the array and name the file with the prefix at the front
        '''
        try:
            ###
            #  initiate the CSV files for each type
            ###
            outFile = self.InitiateCsvFile(batchName, preFx, subFolder)
            csvWriter = csv.writer(outFile, quoting=csv.QUOTE_ALL)
            for oArray in outputHolding:
                outLine = []
                for item in oArray:
                    if isinstance(item, basestring):
                        outLine.append(item.encode('utf-8'))
                    else:
                        outLine.append(item)
                csvWriter.writerow(outLine)
            outFile.close()
        except:
            self.logger.exception(self.moduleName + "- we had an error in CreateCsvFile ")
            raise

    def ContinueProcessJson(self, rec):
        '''
        just a simple test to see if we want to continue processing record
        '''
        try:
            retVal = False
            if "observations" in rec:
                if rec["observations"] is not None:
                    if len(rec["observations"]) > 0:
                        retVal = True
            return retVal
        except:
            self.logger.exception(self.moduleName + "- we had an error in ContinueProcessJson ")
            raise

    def FindVal(self, inRec, inItem):
        '''
        runs thru the json object and searches for the key and if found
        then return the associated value
        '''
        rtnFound = False
        rtnValue = ''
        try:
            for key in inRec.keys():
                if str(key) == str(inItem):
                    rtnFound = True
                    rtnValue = inRec[key]
                    return rtnFound, rtnValue
                if isinstance(inRec[key], dict):
                    rtnFound, rtnValue = self.FindVal(inRec[key], inItem)
        except:
            self.logger.exception(self.moduleName + "- we had an error in FindVal ")
            raise
        return rtnFound, rtnValue

    def CheckValue(self, inRec, inItem, fType):
        '''
        Helper method so that we remove cr lf if they are in the string
        '''
        try:
            found = False
            found, val = self.FindVal(inRec, inItem)
            if found is False:
                return ''
            if fType == 'VARCHAR':
                if isinstance(val, str):
                    val = self.fileUtilities.RemoveSpecialCharsFromString(val)
                    val = re.sub(r'\\', r'\\\\', val)
                elif isinstance(val, unicode):
                    val = self.fileUtilities.RemoveSpecialCharsFromString(val)
                    val = re.sub(r'\\', r'\\\\', val)
            if fType == "DATE":
                if isinstance(val, str):
                    tval = datetime.strptime(val, '%Y-%m-%d')
                    if tval.year < 1900:
                        tval = datetime.strptime('1900-01-01', '%Y-%m-%d')
                    val = tval.strftime('%Y-%m-%d')
                elif isinstance(val, datetime):
                    val = val.strftime('%Y-%m-%d')
            return val
        except:
            self.logger.exception(self.moduleName + "- we had an error in CheckValue - " + inItem)
            raise
    def LoadJData(self, src, jFile):
        '''
        try and load the data into a Json Object
        '''
        jdata = open(src + "/" + jFile).read()
        dataFile = None
        encodeList = [None, "cp1252"]
        for enc in encodeList:
            try:
                if enc is not None:
                    dataFile = json.loads(jdata, encoding=enc)
                else:
                    dataFile = json.loads(jdata)
                break
            except ValueError:
                continue
        if dataFile is None:
            self.logger.exception(self.moduleName + "- we had an error in LoadJData - " + jFile)
            raise Exception('could not load json file')
        else:
            return dataFile

    def ProcessJsonFile(self, src, jfl, batchName):
        '''
        loads the json and then calls the process routine
        '''
        try:
            dataFile = self.LoadJData(src, jfl)
            self.ProcessJson(dataFile, batchName)
        except:
            self.logger.exception(self.moduleName + "- we had an error in ProcessJsonFile with file " + jfl)
            raise

    def LoadAttrArray(self, rec, srcId, keyField='source_id'):
        '''
        return array of attribute values
        '''
        outAttrRecArray = []
        try:
            for fld in self.commonParams["attrFields"]:
                if fld["name"] == keyField:
                    outAttrRecArray.append(srcId)
                else:
                    outAttrRecArray.append(self.CheckValue(rec, fld["name"], fld["type"]))
        except:
            self.logger.exception(self.moduleName + "- we had an error in LoadAttrArray ")
            raise
        return outAttrRecArray

    def ProcessJson(self, dataFile, batchName):
        '''
        process one json file and create two csv files from it
        '''
        try: # pylint: disable=too-many-nested-blocks
            outputAttrHolding = []
            outputDataHolding = []
            ###
            #  gets all the atributes
            ###
            if 'value' in dataFile:
                for rec in dataFile["value"]:
                    if self.ContinueProcessJson(rec) is True:
                        srcId = self.CheckValue(rec, "source_id", "VARCHAR")
                        for obsRec in rec["observations"]:
                            outputDataHolding.append([srcId,
                                                      obsRec["date"],
                                                      obsRec["value"]])

                        outAttrRecArray = self.LoadAttrArray(rec, srcId)
                        outputAttrHolding.append(outAttrRecArray)
            else:
                rec = dataFile
                if self.ContinueProcessJson(rec) is True:
                    srcId = self.CheckValue(rec, "source_id", "VARCHAR")
                    for obsRec in rec["observations"]:
                        outputDataHolding.append([srcId,
                                                  obsRec["date"],
                                                  obsRec["value"]])

                    outAttrRecArray = self.LoadAttrArray(rec, srcId)
                    outputAttrHolding.append(outAttrRecArray)

            ###
            #  now create fill csv file
            ###
            self.CreateCsvFile(batchName, "attr_", outputAttrHolding, "attribute")
            self.CreateCsvFile(batchName, "data_", outputDataHolding, "data")

        except:
            self.logger.exception(self.moduleName + "- we had an error in ProcessJsonFile with batch " + batchName)
            raise

    def ProcessZipContents(self, zFileFolder, batchName):
        '''
        process all the files that were in the zip file
        '''
        try:
            onlyFiles = [fl for fl in listdir(zFileFolder) if isfile(join(zFileFolder, fl))]
            for jfl in onlyFiles:
                self.ProcessJsonFile(zFileFolder, jfl, batchName)
        except:
            self.logger.exception(self.moduleName + "- we had an error in ProcessZipContents with batch " + batchName)
            raise

    def GZipItUp(self, batchFolderName):
        '''
        routine to gzip the csv files and put them in a gzip folder
        '''
        try:
        ###
        #  since we are one csv per batch we can just zip the combined csv
        ###

            FileUtilities.CreateFolder(self.commonParams["gzipFolder"] + "attr/")
            FileUtilities.CreateFolder(self.commonParams["gzipFolder"] + "data/")
            if  self.commonParams["csvFolder"].endswith("/"):
                attrFileNamecsv = self.commonParams["csvFolder"] + 'attribute/' + 'attr_' + batchFolderName + '.csv'
                dataFileNamecsv = self.commonParams["csvFolder"] + 'data/' + 'data_' + batchFolderName + '.csv'
            else:
                attrFileNamecsv = self.commonParams["csvFolder"] + '/attribute/' + 'attr_' + batchFolderName + '.csv'
                dataFileNamecsv = self.commonParams["csvFolder"] + '/data/' + 'data_' + batchFolderName + '.csv'
            if  self.commonParams["gzipFolder"].endswith("/"):
                attrFileNameGz = self.commonParams["gzipFolder"] + "attr/" + 'attr_' + batchFolderName + '.csv.gz'
                dataFileNameGz = self.commonParams["gzipFolder"] + "data/" + 'data_' + batchFolderName + '.csv.gz'
            else:
                attrFileNameGz = self.commonParams["gzipFolder"] + "/attr/" + 'attr_' + batchFolderName + '.csv.gz'
                dataFileNameGz = self.commonParams["gzipFolder"] + "/data/" + 'data_' + batchFolderName + '.csv.gz'

            self.fileUtilities.GzipFile(attrFileNamecsv, attrFileNameGz)
            self.fileUtilities.GzipFile(dataFileNamecsv, dataFileNameGz)
        except:
            self.logger.exception(self.moduleName + "- we had an error in ZipItUp with " + batchFolderName)
            raise

    def StartHere(self):
        '''
        initial starting routine
        '''
        try:
            self.moduleName = self.commonParams["moduleName"]
            self.logger = FileUtilities.CreateLogger(self.commonParams["loggerParams"])
            self.logger.info("zipfile = " + self.fl + " started " + datetime.now().strftime('%Y-%m-%d %I:%M:%S'))
            ###
            #  pull this file to local instance
            ###
            fileName = ntpath.basename(self.fl)
            batchFolderName = re.sub(r'\.zip$', '', fileName)
            ###
            #  make sure we have this folder
            ###
            self.fileUtilities = FileUtilities(self.logger)
            segZipFolder = self.commonParams["zipFolder"] + batchFolderName + "/"
            self.fileUtilities.RemoveFolder(segZipFolder)
            self.fileUtilities.CreateFolder(segZipFolder)
            localGzipFilepath = self.commonParams["localTempDirectory"] + "/" + fileName
            self.fileUtilities.UnzipFile(localGzipFilepath, segZipFolder)
            zipContentFolder = re.sub(r'\/$', '', segZipFolder)
            directories = [fName for fName in os.listdir(segZipFolder) if os.path.isdir(os.path.join(segZipFolder, fName))]
            for dirs in directories:
                zipContentFolder = os.path.join(segZipFolder, dirs)
            self.ProcessZipContents(zipContentFolder, batchFolderName)
            self.GZipItUp(batchFolderName)
            self.logger.info("zipfile = " + self.fl + " finished " + datetime.now().strftime('%Y-%m-%d %I:%M:%S'))
        except:
            self.logger.exception(self.commonParams["moduleName"] + "- we had an error in StartHere")
            raise
