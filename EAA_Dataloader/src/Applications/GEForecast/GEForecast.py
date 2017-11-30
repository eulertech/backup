'''
Created on Aug 2, 2017

@author: VIU53188
@summary: Application pulls the GEForecast data from MongoDB and loads into RedShift
'''

import os
import json
import re
import math
import datetime
from dateutil.relativedelta import relativedelta
from pymongo import MongoClient

from Applications.Magellan.MagellanUtilities import MagellanUtilities
from Applications.Common.ApplicationBase import ApplicationBase

from AACloudTools.RedshiftUtilities import RedshiftUtilities
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.S3Utilities import S3Utilities

class GEForecast(ApplicationBase):
    '''
    look at summary note for description
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(GEForecast, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))
        self.attrFields = None
        self.dataFields = None
        self.startDate = None

    def GatherFields(self, tables):
        '''
        gets a list of all the fields that will be used later
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "GatherFields" + " starting ")
            for tblName in tables:
                if "destName" in tblName:
                    if tblName["type"] == "attributes":
                        self.attrFields = tblName["fields"]
#                        self.attrTable = tblName["table"]
                    elif tblName["type"] == "series":
                        self.dataFields = tblName["fields"]
#                        self.dataTable = tblName["table"]
                    else:
                        self.logger.info("table fields not defined")
            self.logger.debug(self.moduleName + " -- " + "GatherFields" + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in GatherFields")
            raise

    def GetParamsList(self, tblEtl):
        '''
        get the parameter list if one was stored
        '''
        paramsList = []
        try:
            lastRunRecJson = self.etlUtilities.GetLastGoodRun(tblEtl, self.moduleName)
            if lastRunRecJson is not None:
                if lastRunRecJson["params"] is not None:
                    paramsList = json.loads(lastRunRecJson["params"])
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetParamsList")
            raise
        return paramsList

    def GetMongoConnection(self):
        '''
        returns the connection string to MongoDB
        '''
        try:
            client = MongoClient(host=self.job["mongoDBConnectionInfo"]["server"],
                                 port=self.job["mongoDBConnectionInfo"]["port"])
            db = client[self.job["mongoDBConnectionInfo"]["database"]]
            db.authenticate(self.job["mongoDBConnectionInfo"]["user"],
                            self.job["mongoDBConnectionInfo"]["pwd"],
                            mechanism='SCRAM-SHA-1')
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetConnectionString")
            raise
        return client, db

    def GetWhereClause(self, paramsList):
        '''
        returns the where clause to use
        '''
        whereClause = {}
        try:
            currDate = None
            if "lastrun" in paramsList:
                currDate = paramsList["lastrun"]
            else:
                return whereClause

            ###
            #  bump date by one day
            ###
            fromDate = datetime.datetime.strptime(currDate, '%Y-%m-%d') +\
                       datetime.timedelta(days=1)
            fromDate = fromDate.strftime('%Y-%m-%d')
            if fromDate > datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), '%Y-%m-%d'):
                fromDate = datetime.datetime.strftime(datetime.date.today()-datetime.timedelta(days=1), '%Y-%m-%d')
            startdate = datetime.datetime.strptime(fromDate, '%Y-%m-%d')
            whereClause = {"publishedDate":{"$gt" : startdate}}
#            whereClause = {"publishedDate":{"$gt" : startdate}, 'frequencyChar':{'$eq': 'q'}}
#            oid = ObjectId('59649fc57601c33c6d9b34bd')
#            whereClause = {"_id":{"$eq" : oid}, 'frequencyChar':{'$eq': 'a'}}
            self.startDate = startdate.strftime('%Y-%m-%d')
            return whereClause
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetWhereClause")
            raise

    def GetCollection(self, db, name, paramsList):
        '''
        returns a collection
        paramsList is expected to be in the form of
        {"lastRun":"2006-01-14"}
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "GetCollection for " + name + " starting ")
            whereClause = self.GetWhereClause(paramsList)
            collection = db[name].find(filter=whereClause, no_cursor_timeout=True)
            self.logger.debug(self.moduleName + " -- " + "GetCollection for " + name + " finished ")
            return collection
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetCollection for " + name)
            raise

    def PopDatesArray(self, freqChar, startDate, endDate):
        '''
        taking a frequency, startDate and endDate we populate an array of dates
        that covers that range
        '''
        retArray = []
        try:
            dstartDate = datetime.datetime.strptime(startDate, '%Y-%m-%d')
            dendDate = datetime.datetime.strptime(endDate, '%Y-%m-%d')
            currDate = dstartDate
            retArray.append(currDate.strftime('%Y-%m-%d'))
            while currDate < dendDate:
                if freqChar == 'a':
                    currDate = datetime.datetime(currDate.year + 1, currDate.month, currDate.day)
                elif freqChar == 'm':
                    currDate = currDate + relativedelta(months=1)
                elif freqChar == 'q':
                    currDate = currDate + relativedelta(months=3)
                if currDate <= dendDate:
                    retArray.append(currDate.strftime('%Y-%m-%d'))
        except:
            self.logger.exception(self.moduleName + " - we had an error in PopDatesArray ")
            raise
        return retArray

    def OutputArrayToCSV(self, outputDataHolding, outputAttrHolding, recNdx, muObject):
        '''
        if there are values in the holding arrays then it will create a csv file
        with the name in the form of "attr_recNdx.csv" in its associated folder
        '''
        try:
            lenData = len(outputDataHolding)
            if lenData > 0:
                muObject.CreateCsvFile(str(recNdx), "data_", outputDataHolding, "data")
                muObject.CreateCsvFile(str(recNdx), "attr_", outputAttrHolding, "attribute")
                outputAttrHolding = []
                outputDataHolding = []
        except:
            self.logger.exception(self.moduleName + " - we had an error in OutputArrayToCSV ")
            raise
        return outputAttrHolding, outputDataHolding

    def ProcessMongoCollection(self, collection, muObject, cat):
        '''
        Process the current collection and create csv files.
        after each time a csv file is created we reset the arrays
        just to make sure that we actually do get to process all the values
        when we are done we return the arrays.
        '''
        outputAttrHolding = []
        outputDataHolding = []
        try:
            self.logger.debug(self.moduleName + " -- " + "ProcessMongoCollection "  + " starting ")
            recNdx = 0
            batchNdx = 0
            for masterRec in collection:
                batchNdx = batchNdx + 1
                objectID = muObject.CheckValue(masterRec, "_id", "VARCHAR")
                outAttrRecArray = muObject.LoadAttrArray(masterRec, objectID, keyField='object_id')
                outputAttrHolding.append(outAttrRecArray)
                datesArray = self.PopDatesArray(muObject.CheckValue(masterRec, "frequencyChar", "VARCHAR"),
                                                muObject.CheckValue(masterRec, "startDate", "DATE"),
                                                muObject.CheckValue(masterRec, "endDate", "DATE"))
                dateNdx = 0
                #db.getCollection('giif').find({'_id':{'$eq': ObjectId('59649fc57601c33c6d9b34bd')}})
                for vRec in masterRec["values"]:
                    if math.isnan(vRec) is False:
                        outputDataHolding.append([objectID,
                                                  datesArray[dateNdx],
                                                  vRec])
                    dateNdx = dateNdx + 1
                recNdx = recNdx + 1
                if batchNdx >= self.job["batchsize"]:
                    outputAttrHolding, outputDataHolding = self.OutputArrayToCSV(outputDataHolding,
                                                                                 outputAttrHolding,
                                                                                 cat + str(recNdx), muObject)
                    batchNdx = 0
        except:
            self.logger.exception(self.moduleName + "- we had an error in ProcessMongoCollection ")
            raise
        self.logger.debug(self.moduleName + " -- " + "ProcessMongoCollection "  + " finished ")
        return outputAttrHolding, outputDataHolding

    def PullData(self, cat, paramsList, muHelper):
        '''
        pull data from a specific category in MongoDB
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "PullData for " + cat + " starting ")
            conn, db = self.GetMongoConnection()
            collection = self.GetCollection(db, cat, paramsList)
            outputAttrHolding = []
            outputDataHolding = []

            outputAttrHolding, outputDataHolding = self.ProcessMongoCollection(collection, muHelper, cat)
            ###
            #  just in case we did not output all the data we do one final check
            ###
            outputAttrHolding, outputDataHolding = self.OutputArrayToCSV(outputDataHolding,
                                                                         outputAttrHolding,
                                                                         cat, muHelper)
            conn.close()
            self.logger.debug(self.moduleName + " -- " + "PullData for " + cat + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in PullData")
            raise

    def MoveToS3(self, localFolderName, folderName, subFolder):
        '''
        move gzip files to s3 and clean local instance
        localFolderName --> local folder name
        subFolder --> date
        folderName --> folder name on s3
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "MoveToS3 "  + localFolderName + " starting ")
            ###
            #  move any gzip files to the s3 server
            ###
            s3folder = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"] +\
                        "/" + folderName + '/' + subFolder
            localFolder = self.fileUtilities.gzipFolder + localFolderName
            S3Utilities.SyncFolderAWSCli(localFolder,
                                         s3folder,
                                         args='''--quiet --include "*.gz"''', dbug="Y")
            self.logger.debug(self.moduleName + " -- " + "MoveToS3 "  + localFolderName + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in MoveToS3")
            raise

    def LoadData(self, folderName, subFolder, tblJson):
        '''
        load the data from s3 into RedShift
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "LoadData" + " starting ")
            rsConnect = self.etlUtilities.GetAWSConnection(self.awsParams)

            s3folder = "s3://" + self.job["bucketName"] + self.job["s3GzipFolderBase"] +\
                        "/" + folderName + '/' + subFolder
            RedshiftUtilities.LoadDataFromS3(rsConnect, self.awsParams.s3,
                                             {
                                                 "destinationSchema": self.job["destinationSchema"],
                                                 "tableName": tblJson["table"],
                                                 "s3Filename": s3folder,
                                                 "fileFormat": self.job["fileFormat"],
                                                 "dateFormat": self.job["dateFormat"],
                                                 "delimiter": self.job["delimiter"]
                                             },
                                             self.logger, "N")
            self.logger.info(self.moduleName + " - Finished loading s3 data to " +\
                             self.job["destinationSchema"] + '.' +  tblJson["table"])
            rsConnect.close()
            self.logger.debug(self.moduleName + " -- " + "LoadData" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in LoadData")
            raise

    def MoveFromCSVToGzip(self, cat):
        '''
        move files from the CSV folder to GZ folders
        '''
        try:
            self.logger.debug(self.moduleName + " -- MoveFromCSVToGzip for " + cat + " starting ")

            inputFolder = self.fileUtilities.csvFolder + "attribute/"
            outputFolder = self.fileUtilities.gzipFolder + "attribute/"
            self.fileUtilities.EmptyFolderContents(outputFolder)
            self.fileUtilities.GzipMultipleFiles(inputFolder, outputFolder)
            inputFolder = self.fileUtilities.csvFolder + "data/"
            outputFolder = self.fileUtilities.gzipFolder + "data/"
            self.fileUtilities.EmptyFolderContents(outputFolder)
            self.fileUtilities.GzipMultipleFiles(inputFolder, outputFolder)
            self.fileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder + "attribute/")
            self.fileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder + "data/")
            self.logger.debug(self.moduleName + " -- MoveFromCSVToGzip for " + cat + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in MoveFromCSVToGzip")
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

    def MoveFileToServer(self, fromDate, tblJson, cat):
        '''
        if there are any files that need to be moved to S3 from the local instance do it
        '''
        try:
###
#  do I even have any files to process
###
            numFiles = self.fileUtilities.GetListOfFiles(self.fileUtilities.gzipFolder + tblJson["s3subfolder"] + '/')
            lenNumFiles = len(numFiles)
            if lenNumFiles > 0:
                self.MoveToS3(tblJson["s3subfolder"], fromDate + '/' + cat, tblJson["s3subfolder"])
                self.LoadData(fromDate + '/' + cat, tblJson["s3subfolder"], tblJson)
                self.fileUtilities.EmptyFolderContents(self.fileUtilities.gzipFolder + tblJson["s3subfolder"] + '/')
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetFromDate")
            raise

    def CreateUpdateScript(self, pEtlSchema, pEtlTable, currProcId):
        '''
        takes the template for the Update sqlscript and customizes it
        '''
        sqlUpdateScript = None
        try:
            self.logger.debug(self.moduleName + " -- " + "UpDate Table Script" + " starting ")
            sqlUpdateTemplate = self.location + '/sql/' + self.job["sqlUpdateScript"]
            sqlUpdateScript = self.localTempDirectory + "/sql/" + re.sub('Template.sql$', '.sql', self.job["sqlUpdateScript"])

            FileUtilities.RemoveFileIfItExists(sqlUpdateScript)

            with open(sqlUpdateTemplate) as infile, open(sqlUpdateScript, 'w') as outfile:
                for line in infile:
                    line = line.replace('{schemaName}', self.job["destinationSchema"])
                    line = line.replace('{tbstats}', pEtlSchema + "." + pEtlTable)
                    line = line.replace('{procid}', str(currProcId))
                    outfile.write(line)
            self.logger.debug(self.moduleName + " -- " + "UpDate Table Script" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in UpDate Table Script")
            raise
        return sqlUpdateScript

    def UpdateTable(self, pEtlSchema, pEtlTable, currProcId):
        '''
        Routine to create and run the sql to create version in RedShift
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "Update tables for " + pEtlSchema + "." + pEtlTable + " starting ")
            verScript = self.CreateUpdateScript(pEtlSchema, pEtlTable, currProcId)
            self.logger.info(self.moduleName + " - script file name = " + verScript)
            RedshiftUtilities.PSqlExecute(verScript, self.logger)
            self.logger.debug(self.moduleName + " -- " + "Update tables for " + pEtlSchema + "." + pEtlTable + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in UpdateTable")
            raise

    def Start(self, logger, moduleName, filelocs):
        '''
        Start of routine
        '''
        currProcId = None
        try:
            ApplicationBase.Start(self, logger, moduleName, filelocs)
            self.logger.debug(self.moduleName + " -- " + " starting ")
            currProcId = self.etlUtilities.GetRunID(filelocs["tblEtl"]["table"], self.moduleName)
            lastRunRecJson = self.etlUtilities.GetLastGoodRun(filelocs["tblEtl"]["table"], self.moduleName)
            paramsList = []
            if lastRunRecJson is not None:
                if lastRunRecJson["params"]:
                    paramsList = json.loads(lastRunRecJson["params"])
###
#  set up to run create folder
###
            self.fileUtilities.moduleName = self.moduleName
            self.fileUtilities.localBaseDirectory = self.localTempDirectory
            self.fileUtilities.CreateFolders(self.job["folders"])
###
            for cat in self.job["categories"]:
                self.logger.debug(self.moduleName + " -- category " + cat["name"] + " starting ")
                self.logger.debug(self.moduleName + " -- create tables for category " + cat["name"] + " starting ")
                for tblJson in cat["tables"]:
                    fname = self.fileUtilities.CreateTableSql(tblJson, self.fileUtilities.sqlFolder)
                    RedshiftUtilities.PSqlExecute(fname, self.logger)
                self.GatherFields(cat["tables"])
                self.logger.debug(self.moduleName + " -- create tables for category " + cat["name"] + " finished ")

                muHelper = MagellanUtilities()
                muHelper.logger = self.logger
                muHelper.fileUtilities = self.fileUtilities
                commonParams = {}
                commonParams["csvFolder"] = self.fileUtilities.csvFolder
                commonParams["gzipFolder"] = self.fileUtilities.gzipFolder
                commonParams["attrFields"] = self.attrFields
                muHelper.commonParams = commonParams
                self.PullData(cat["name"], paramsList, muHelper)
                self.MoveFromCSVToGzip(cat["name"])
                fromDate = self.GetFromDate()
                for tblJson in cat["tables"]:
                    if "destName" in tblJson:
                        self.MoveFileToServer(fromDate, tblJson, cat["name"])

            if self.etlUtilities.SetInstanceParameters(filelocs["tblEtl"]["table"],\
                                                       currProcId,\
                                                       json.dumps(
                                                               {"lastrun":fromDate}
                                                           )) is not True:
                self.logger.info(self.moduleName + " - we could not set the instance.")
            self.UpdateTable(filelocs["tblEtl"]["schemaName"], filelocs["tblEtl"]["table"], currProcId)
            if self.job["cleanlocal"] == "Y":
                self.fileUtilities.RemoveFolder(self.localTempDirectory)
            self.logger.debug(self.moduleName + " -- " + " finished ")
        except Exception as err:
            self.logger.exception(moduleName + " - Exception! Error: " + err.message)
            if self.etlUtilities.CompleteInstance(filelocs["tblEtl"]["table"], currProcId, 'F') is not True:
                self.logger.info(self.moduleName + " - we could not Complete Instance.")
            raise Exception(err.message)
