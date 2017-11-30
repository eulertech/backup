'''
Created on Oct 31, 2017

@author: VIU53188
@summary: Application pulls the GEForecast data from MongoDB and loads into RedShift modified
        to work with Athena and Spark
'''

import os
import sys
import datetime
import math
from dateutil.relativedelta import relativedelta

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.MongoDBUtilities import MongoDBUtilities

from AACloudTools.AthenaUtilities import AthenaUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from Applications.Magellan.MagellanUtilities import MagellanUtilities
from Applications.Common.ApplicationBase import ApplicationBase
#pylint: disable=W0223
class GEForecastAthenaSpark(ApplicationBase):
    '''
    look at summary note for description
    '''
    def __init__(self):
        '''
        Initial settings
        '''
        super(GEForecastAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(
            os.path.dirname(os.path.abspath(__file__)))
        self.attrFields = None
        self.dataFields = None

    def GetMaxPublishDateInAthena(self, catalog):
        '''
        Get the last year month (based on valuation date) that has been process in Athena
        '''
        try:
            athenaSchemaName = AthenaUtilities.ComposeAthenaSchemaName(catalog["schemaName"])
            maxDate = AthenaUtilities.GetMaxValue(self.awsParams,
                                                  athenaSchemaName,
                                                  catalog["paramTable"],
                                                  "publisheddate", self.logger)
            if maxDate == 'max_val':
                maxDate = None
        except ValueError:
            maxDate = None   #'2017-10-10' # Some really low value in case the table has not been created yet
        except:
            raise

        return maxDate

    def GatherFields(self, tables):
        '''
        gets a list of all the fields that will be used later
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "GatherFields" + " starting ")
            for tblName in tables:
                if tblName["type"] == "attributes":
                    self.attrFields = tblName["fields"]
                elif tblName["type"] == "series":
                    self.dataFields = tblName["fields"]
                else:
                    self.logger.info("table fields not defined")
            self.logger.debug(self.moduleName + " -- " + "GatherFields" + " finished ")
        except:
            self.logger.exception(self.moduleName + "- we had an error in GatherFields")
            raise

    def GetWhereClause(self, maxDate):
        '''
        returns the where clause to use
        '''
        whereClause = {}
        try:
            currDate = None
            if maxDate is None:
                return whereClause
            else:
                currDate = maxDate
            ###
            #  bump date by one day
            ###
            fromDate = datetime.datetime.strptime(currDate, '%Y-%m-%d').date() +\
                       datetime.timedelta(days=1)
            if fromDate > datetime.date.today()-datetime.timedelta(days=1):
                fromDate = datetime.date.today()-datetime.timedelta(days=1)

#            fromDate = fromDate.strftime('%Y-%m-%d')
            startdate = datetime.datetime.strptime(fromDate.strftime('%Y-%m-%d'), '%Y-%m-%d')
            whereClause = {"publishedDate":{"$gt" : startdate}}
#            db.getCollection('giif').find({'publishedDate': {'$gt': new ISODate("2017-10-10")}})
#            whereClause = {"publishedDate":{"$gt" : startdate}, 'frequencyChar':{'$eq': 'q'}}
#            oid = ObjectId('59649fc57601c33c6d9b34bd')
#            whereClause = {"_id":{"$eq" : oid}, 'frequencyChar':{'$eq': 'a'}}
            return whereClause
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetWhereClause")
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

    def OutputArrayToCSV(self, outputDataHolding, outputAttrHolding, recNdx, muObject, catalog):#pylint: disable=too-many-arguments
        '''
        if there are values in the holding arrays then it will create a csv file
        with the name in the form of "attr_recNdx.csv" in its associated folder
        '''
        try:
            lenData = len(outputDataHolding)
            if lenData > 0:
                for tables in catalog["tables"]:
                    if tables["type"] == 'series':
                        muObject.CreateCsvFile(str(recNdx), "data_", outputDataHolding, "data")
                    elif tables["type"] == 'attributes':
                        muObject.CreateCsvFile(str(recNdx), "attr_", outputAttrHolding, "attribute")
                outputAttrHolding = []
                outputDataHolding = []
        except:
            self.logger.exception(self.moduleName + " - we had an error in OutputArrayToCSV ")
            raise
        return outputAttrHolding, outputDataHolding

    def CreateParquetFilesAndLoad(self, catalog, partitionValue):
        '''
        Creates the parquet files
        '''
        try:
            spark = SparkUtilities.GetCreateSparkSession(self.logger)
            for tables in catalog["tables"]:
                if tables["type"] == "attributes":
                    srcFolder = self.fileUtilities.csvFolder+'/attribute/'
                else:
                    srcFolder = self.fileUtilities.csvFolder+'/data/'
                tableSchema = SparkUtilities.BuildSparkSchema(tables)
 
                df = (spark.read
                      .format("com.databricks.spark.csv")
                      .options(header=False, delimiter=self.job["delimiter"])
                      .schema(tableSchema)
                      .load(srcFolder)
                     )
                SparkUtilities.SaveParquet(df, self.fileUtilities)
                self.UploadFilesCreateAthenaTablesAndSqlScripts(tables, self.fileUtilities.parquet, partitionValue)
                self.fileUtilities.EmptyFolderContents(self.fileUtilities.parquet)
                self.fileUtilities.EmptyFolderContents(srcFolder)
            
        except Exception as ex:
            self.logger.exception(self.moduleName + " - we had an error in CreateParquetFilesAndLoad " + ex.message)
            raise
            
            
    def ProcessMongoCollection(self, collection, muObject, cat, catalog, maxDate):
        '''
        Process the current collection and create csv files.
        after each time a csv file is created we reset the arrays
        just to make sure that we actually do get to process all the values
        when we are done we return the arrays.
        '''
        outputAttrHolding = []
        outputDataHolding = []
        if collection.count() > 0:
            baseDateCollection = collection.distinct("publishedDate")
            baseDateCollection.sort()
            baseDate = baseDateCollection[0].strftime('%Y-%m-%d')
        else:
            baseDate = maxDate
        try:
            self.logger.debug(self.moduleName + " -- " + "ProcessMongoCollection "  + " starting ")
            numRecs = 0
            for masterRec in collection:
                if baseDate is None:
                    baseDate = muObject.CheckValue(masterRec, "publishedDate", "DATE")
                if baseDate < muObject.CheckValue(masterRec, "publishedDate", "DATE"):
                    numRecs = len(outputDataHolding)
                    if numRecs > 0:
                        outputAttrHolding, outputDataHolding = self.OutputArrayToCSV(outputDataHolding,
                                                                                     outputAttrHolding,
                                                                                     cat + baseDate, muObject, catalog)
                        self.CreateParquetFilesAndLoad(catalog, baseDate)
                        
                        baseDate = muObject.CheckValue(masterRec, "publishedDate", "DATE")  
                        for tables in catalog["tables"]:
                            if "loadToRedshift" in tables:
                                if tables["loadToRedshift"] == "Y":
                                    self.LoadDataFromAthenaIntoRedShiftLocalScripts(tables)

                objectID = muObject.CheckValue(masterRec, "_id", "VARCHAR")
                outAttrRecArray = muObject.LoadAttrArray(masterRec, objectID, keyField='object_id')
                outputAttrHolding.append(outAttrRecArray)
                datesArray = self.PopDatesArray(muObject.CheckValue(masterRec, "frequencyChar", "VARCHAR"),
                                                muObject.CheckValue(masterRec, "startDate", "DATE"),
                                                muObject.CheckValue(masterRec, "endDate", "DATE"))
                dateNdx = 0
                for vRec in masterRec["values"]:
                    if math.isnan(vRec) is False:
                        outputDataHolding.append([objectID,
                                                  datesArray[dateNdx],
                                                  vRec])
                    dateNdx = dateNdx + 1

        except:
            self.logger.exception(self.moduleName + "- we had an error in ProcessMongoCollection ")
            raise
        self.logger.debug(self.moduleName + " -- " + "ProcessMongoCollection "  + " finished ")
        return outputAttrHolding, outputDataHolding, baseDate

    def ProcessData(self, maxDate, muHelper, catalog, dbCommon):
        '''
        pull data from a specific category in MongoDB
        '''
        try:
            if sys.version[0] == '2':
                cat = str(catalog["name"])
            elif sys.version[0] == '3':
                cat = catalog["name"]
            self.logger.debug(self.moduleName + " -- " + "ProcessData for " + cat + " starting ")
            conn, db = MongoDBUtilities.GetMongoConnection(dbCommon)
            whereClause = self.GetWhereClause(maxDate)
            sortCondition = {}
            sortCondition["field"] = "publishedDate"
            sortCondition["order"] = "asc"
            collection = MongoDBUtilities.GetCollection(db, cat, whereClause, sortCondition=sortCondition)
            outputAttrHolding = []
            outputDataHolding = []

            outputAttrHolding, outputDataHolding, maxDate = self.ProcessMongoCollection(collection, muHelper, cat, catalog, maxDate)
            ##
            #  just in case we did not output all the data we do one final check
            ##
            numRecs = len(outputDataHolding)
            if numRecs > 0:
                outputAttrHolding, outputDataHolding = self.OutputArrayToCSV(outputDataHolding,
                                                                             outputAttrHolding,
                                                                             cat + maxDate, muHelper, catalog)
                self.CreateParquetFilesAndLoad(catalog, maxDate)
                for tables in catalog["tables"]:
                    if "loadToRedshift" in tables:
                        if tables["loadToRedshift"] == "Y":
                            self.LoadDataFromAthenaIntoRedShiftLocalScripts(tables)
            
            conn.close()
            self.logger.debug(self.moduleName + " -- " + "ProcessData for " + cat + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessData")
            raise

    def ProcessCatalogs(self, dbCommon, catalog):
        '''
        pulls data from each catalog in MongoDB
        '''
        try:
            self.fileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder+'/data/')
            self.fileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder+'/attribute/')

            self.logger.debug(self.moduleName + " -- category " + catalog["name"] + " starting ")
            self.logger.debug(self.moduleName + " -- create tables for category " + catalog["name"] + " starting ")
            maxDate = self.GetMaxPublishDateInAthena(catalog)
            self.GatherFields(catalog["tables"])
            self.logger.debug(self.moduleName + " -- create tables for category " + catalog["name"] + " finished ")
            muHelper = MagellanUtilities()
            muHelper.logger = self.logger
            muHelper.fileUtilities = self.fileUtilities
            commonParams = {}
            commonParams["csvFolder"] = self.fileUtilities.csvFolder
            commonParams["gzipFolder"] = self.fileUtilities.gzipFolder
            commonParams["attrFields"] = self.attrFields
            muHelper.commonParams = commonParams
            self.ProcessData(maxDate, muHelper, catalog, dbCommon)
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessCatalogs for " + catalog["name"])
            raise

    def Start(self, logger, moduleName, filelocs):
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)
