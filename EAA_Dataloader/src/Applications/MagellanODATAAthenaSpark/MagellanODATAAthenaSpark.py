'''
Created on Nov 17, 2017
@author: Hector Hernandez
@summary: This application will pull Magellan data from the MarkLogic system AKA ODATA and is used for incremental updates
        Code was modifed from MagellanODATA to use the Athena and Spark framework
'''
import os, requests, json, re, datetime, time

from Applications.Common.ApplicationBase import ApplicationBase
from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from AACloudTools.AthenaUtilities import AthenaUtilities

class MagellanODATAAthenaSpark(ApplicationBase): #pylint:disable=abstract-method
    '''
    This application will pull Magellan data from the MarkLogic system AKA ODATA
    '''
    def __init__(self):
        '''
        Constructor
        '''
        super(MagellanODATAAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))

    def GetRecordCount(self, dbCommon, oDFilter):
        '''
        Find out how many records are stored in the set
        '''
        retVal = 0

        try:
            url = dbCommon["endpoint"] + dbCommon["name"]  + '/$count' + oDFilter
            req = requests.get(url)

            if req.status_code == 200:
                retVal = req.text
        except Exception as err:
            self.logger.exception(self.moduleName + "- we had an error in GetRecordCount. Message:" + err.message)
            raise

        return retVal

    def GetLastUpdateDate(self, table):
        '''
        Get the last Update Date registered in Athena
        '''
        try:
            athenaSchemaName = AthenaUtilities.ComposeAthenaSchemaName(table["schemaName"])

            last_update_date = AthenaUtilities.GetMaxValue(self.awsParams,
                                                        athenaSchemaName,
                                                        table["table"],
                                                        "etl_last_update_date",
                                                        self.logger)

            if last_update_date is None:
                last_update_date = datetime.date.today() - datetime.timedelta(days=2) #To process from yesterday by default
        except StandardError as err:
            self.logger.info(self.moduleName + " - GetLastUpdateDate() Error: " + err.message)

        return datetime.datetime.strptime(str(last_update_date), "%Y-%m-%d").date()

    def GetFilter(self, last_update_date):
        '''
        Returns the filter to be applied for the OData call...
        '''
        oDFilter = "?$filter=last_update ge {}T00:00Z and last_update le {}T23:59Z".format(last_update_date, last_update_date)
        oDFilter += "&document_type eq 'Timeseries'"

        return oDFilter

    def CheckValue(self, val, fType):
        '''
        Helper method so that we remove cr lf if they are in the string
        '''
        try:
            if fType == 'VARCHAR':
                if isinstance(val, str):
                    val = self.fileUtilities.RemoveSpecialCharsFromString(val)
                    val = re.sub(r'\\', r'\\\\', val)
                elif isinstance(val, unicode):
                    val = self.fileUtilities.RemoveSpecialCharsFromString(val)
                    val = re.sub(r'\\', r'\\\\', val)
                elif isinstance(val, int):
                    val = str(val)
            if fType == "DATE":
                if isinstance(val, str):
                    tval = datetime.datetime.strptime(val, '%Y-%m-%d')
                    if tval.year < 1900:
                        tval = datetime.datetime.strptime('1900-01-01', '%Y-%m-%d')
                    val = tval.strftime('%Y-%m-%d')
                elif isinstance(val, datetime.datetime):
                    val = val.strftime('%Y-%m-%d')
            return val
        except:
            self.logger.exception(self.moduleName + "- we had an error in CheckValue - " + val)
            raise

    def ExtractToJSONLines(self, data, attrsDDL, last_update_date):
        '''
        Extracts the data into attributes and series JSON Lines files.
        '''
        sLast_update_date = str(last_update_date)

        with open(self.localTempDirectory + "/raw/magellan_attributes.json", "a+") as attrsFile:
            for oDataValue in data["value"]:
                newJSONLine = {}

                for aField in attrsDDL["fields"]:
                    if "athenaOnly" in aField:
                        if aField["athenaOnly"] == "Y":
                            continue

                    if oDataValue[aField["name"]] is not None:
                        try:
                            newJSONLine[aField["name"]] = self.CheckValue(oDataValue[aField["name"]], aField["type"])
                        except Exception as err:
                            print(err.message)
                            raise
                    else:
                        newJSONLine[aField["name"]] = ""

                newJSONLine["etl_last_update_date"] = sLast_update_date
                attrsFile.write('%s\n' % (json.dumps(newJSONLine)))

                with open(self.localTempDirectory + "/raw/magellan_series.json", "a+") as seriesFile:
                    for dataSerie in oDataValue["observations"]:
                        newJSONLine = {}
                        newJSONLine["source_id"] = oDataValue["source_id"]
                        newJSONLine["date"] = dataSerie["date"]
                        newJSONLine["value"] = dataSerie["value"]
                        newJSONLine["etl_last_update_date"] = sLast_update_date

                        seriesFile.write('%s\n' % (json.dumps(newJSONLine)))

    def PullOData(self, dbCommon, attrsDDL, last_update_date):
        '''
        Gets the data from the OData API. 
        '''
        skiprecs = 0
        oDFilter = self.GetFilter(last_update_date)
        expectedRecs = int(self.GetRecordCount(dbCommon, oDFilter))

        if expectedRecs % dbCommon["ODATAbatchsize"] == 0:
            numIterations = expectedRecs / dbCommon["ODATAbatchsize"]
        else:
            numIterations = expectedRecs / dbCommon["ODATAbatchsize"] + 1
        
        self.logger.info(self.moduleName + " - Expecting Recs: " + str(expectedRecs) + ", Iterations: " + str(numIterations))

        for ndx in range(0, numIterations):
            requestReturned = False
            reqAttemptCount = 0
            skiprecs = ndx * dbCommon["ODATAbatchsize"]

            if skiprecs > expectedRecs:
                break

            while not requestReturned:
                try:
                    url = dbCommon["endpoint"] + dbCommon["name"] + oDFilter + '&$orderby=source_id' + '&$expand=observations' + \
                        '&$top=' + str(dbCommon["ODATAbatchsize"]) + '&$skip=' + str(skiprecs)
                    
                    self.logger.info(self.moduleName + " - Pulling from: " + url)

                    req = requests.get(url)
    
                    if req.status_code == 200:
                        data = json.loads(req.text)
                        dataLen = len(data["value"])

                        if dataLen > 0:
                            self.ExtractToJSONLines(data, attrsDDL, last_update_date)
                        else:
                            self.logger.exception(self.moduleName + "- OData Completed as No Data for segment:")

                        requestReturned = True
                    else:
                        self.logger.exception(self.moduleName + "- OData error returned:" + str(req.status_code))
                        requestReturned = True
                except StandardError:
                    reqAttemptCount += 1
    
                    if reqAttemptCount > dbCommon["maxretries"]:
                        self.logger.debug(self.moduleName + "- Max retries exceeded.")
                        requestReturned = True
                        raise 
                    else:
                        time.sleep(dbCommon["sleepBeforeRetry"])
                        continue

    def ProcessCatalogs(self, dbCommon, catalog):
        '''
        Process each risks table.
        '''
        try:
            last_update_date = self.GetLastUpdateDate(catalog["tables"][0])

            for i in range(dbCommon["daysback"]):
                last_update_date += datetime.timedelta(days=1)

                if last_update_date < datetime.date.today():
                    self.logger.info(self.moduleName + "- Processing Date: " + str(last_update_date))

                    self.fileUtilities.EmptyFolderContents(self.localTempDirectory + "/raw")
                    self.PullOData(dbCommon, catalog["tables"][0], last_update_date)
                    spark = SparkUtilities.GetCreateSparkSession(self.logger)

                    for tableDDL in catalog["tables"]:
                        fileBaseName = "last_update_date-" + str(last_update_date)
                        df = spark.read.json(self.localTempDirectory + "/raw/magellan_" + tableDDL["type"] + ".json")

                        SparkUtilities.SaveParquet(df, self.fileUtilities, fileBaseName)
                        self.UploadFilesCreateAthenaTablesAndSqlScripts(tableDDL, self.fileUtilities.parquet, str(last_update_date))
                else:
                    self.logger.info(self.moduleName + "- Already up to date. " + str(i))
        except Exception as err:
            self.logger.error(self.moduleName + " - Error while trying to process catalog. Error: " + err.message)
            raise

    def Start(self, logger, moduleName, filelocs):
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)
