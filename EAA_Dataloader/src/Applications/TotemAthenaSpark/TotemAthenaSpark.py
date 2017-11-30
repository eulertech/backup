'''
Created on Nov 1, 2017

@author: VIU53188
@summary: Modified from the original Totem application to utilize Spark and Athena
'''
import os
import datetime
import urllib
import urllib2

from xml.dom.expatbuilder import parseString
import requests
import untangle

from AACloudTools.FileUtilities import FileUtilities
from AACloudTools.SparkUtilities import SparkUtilities
from AACloudTools.AthenaUtilities import AthenaUtilities
from Applications.Common.ApplicationBase import ApplicationBase

class TotemAthenaSpark(ApplicationBase):
    '''
    classdocs
    '''
        # pylint: disable=too-many-instance-attributes
    def __init__(self):
        super(TotemAthenaSpark, self).__init__()
        self.location = FileUtilities.PathToForwardSlash(os.path.dirname(os.path.abspath(__file__)))
        self.logonCreds = {}
        self.baseUrl = None
        self.sourceTableName = None
        self.currProcId = None
        self.fromDate = None

    def GetOutputFilename(self, infileName, inExt='csv'):
        '''
        gets the file name to use
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "GetOutputFilename" + " starting ")
            fnamesplitarray = infileName.rsplit('.', 1)
            newfilename = infileName.replace(fnamesplitarray[1], inExt, 1)
            self.logger.debug(self.moduleName + " -- " + "GetOutputFilename" + " finished ")
            return newfilename
        except:
            self.logger.exception('%s - we had an error in TotemPy during GetOutputFilename' % (self.moduleName))
            raise

    def Convert2CSV(self, fstream, fname):
        '''
        converts the data aka xml to a csv file and then gzips the file
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "Convert2CSV" + " starting " + fname)
            csvfileName = self.fileUtilities.csvFolder + self.GetOutputFilename(fname, 'csv')
            obj = untangle.parse(fstream)
            if self.IterateOverObject(obj, csvfileName) is True:
                csvFileWasCreated = True
            self.logger.debug(self.moduleName + " -- " + "Convert2CSV" + " finished " + fname)
        except:
            self.logger.exception('%s - we had an error in convert2CSV with %s' % (self.moduleName, fname))
            raise

    def IterateOverObject(self, jsonobj, outfile):
        '''
        this is the routine that actually creates the csv file
        '''
        # pylint: disable=too-many-nested-blocks
        # pylint: disable=too-many-locals
        # pylint: disable=too-many-branches
        import csv
        def CheckValue(obj, name):
            '''
            checks the value to see if it is valid
            '''
            if hasattr(obj, name):
                return obj.__dict__.get(name).cdata
            return ""

        retVal = False
        try:
            self.logger.debug(self.moduleName + " -- " + "IterateOverObject" + " starting ")
            with open(outfile, 'wb') as csvfile:
                cw = csv.writer(csvfile, delimiter=',', quoting=csv.QUOTE_ALL)
                l1 = []
                l2 = []
                l3 = []
                for res in jsonobj.totem.results:
                    l1 = []
                    for l1Item in self.job["level1items"]:
                        l1.append(CheckValue(res, l1Item["name"]))
                    if hasattr(res, 'energy'):
                        for egr in res.energy:
                            if hasattr(egr, 'underlier'):
                                for und in egr.underlier:
                                    l2 = []
                                    for l2item in self.job["level2items"]:
                                        l2.append(CheckValue(und, l2item["name"]))
                                    for inst in und.instrument:
                                        l3 = []
                                        for l3item in self.job["level3items"]:
                                            l3.append(CheckValue(inst, l3item["name"]))
                                        row = []
                                        for part1 in l1:
                                            row.append(unicode(part1).encode("utf-8"))
                                        for part2 in l2:
                                            row.append(unicode(part2).encode("utf-8"))
                                        for part3 in l3:
                                            row.append(unicode(part3).encode("utf-8"))
                                        cw.writerow(row)
                                retVal = True
                            else:
                                self.logger.error(self.moduleName + " - iterateoverobject invalid xlm node underlier not found")
                                break
                    else:
                        self.logger.error(self.moduleName + " - iterateoverobject invalid xlm node energy not found")
                        break

        except Exception as ex:  # pylint: disable=broad-except
            self.logger.exception(self.moduleName + " - we had an error in iterateoverobject " + ex.message)
        self.logger.debug(self.moduleName + " -- " + "IterateOverObject" + " finished ")
        return retVal

    def SetGlobalVariables(self):
        '''
        used to separate out process to populate local variables
        '''
        self.logonCreds = {
            'USERNAME': self.job["General"]["user"],
            'PASSWORD': self.job["General"]["pwd"]
        }
        self.baseUrl = self.job["General"]["baseurl"] + "/" +\
            self.job["DatesParams"]["service_type"] + \
            "/" + self.job["DatesParams"]["product"] +\
            "/" + self.job["DatesParams"]["service"]
                
    def GetLatestValuationDateInAthena(self, table):
        '''
        Get the last year month (based on valuation date) that has been process in Athena
        '''
        try:
            athenaSchemaName = AthenaUtilities.ComposeAthenaSchemaName(table["schemaName"])
            latestValuationDateInAthena = AthenaUtilities.GetMaxValue(self.awsParams, athenaSchemaName, table["table"], "etl_valuationdate", self.logger)
        except ValueError:
            latestValuationDateInAthena = None # Some really low value in case the table has not been created yet
        except:
            raise
        
        return latestValuationDateInAthena

    @staticmethod
    def TestCodeDownloadFromURL(url, credentials, outputFileName, logger):
        '''
        Refactor so that we have one function to download
        '''
        try:
            req = requests.post(url, data=credentials)
            if req.status_code == 200:
                with open(outputFileName, "w") as outFile:
                    outFile.write(req.text)
            else:
                errorMsg = "HTTP Code: " + str(req.status_code) + " Message: " + str(req.text)
                logger.exception("DownloadFromURL - " + errorMsg)
                raise Exception(errorMsg)
        except:
            logger.exception("DownloadFromURL - error.")
            raise
        
    def GetAvailableYearMonthsInTotem(self):
        '''
        Get list of all the available year-months available in Totem
        '''
        returnValue = []
        url = self.baseUrl +  "/browse/results/dirlist"
        try:
            self.logger.debug(self.moduleName + " -- " + "GetDates" + " starting ")

            req = requests.post(url, data=self.logonCreds)

            if req.status_code == 200:
                xmldoc = parseString(req.text)
                itemlist = xmldoc.getElementsByTagName('directory')
                for elm in itemlist:
                    for cn in elm.childNodes:
                        returnValue.append(cn.nodeValue.encode())
            else:
                errorMsg = "HTTP Code: " + str(req.status_code) + " Message: " + str(req.text)
                self.logger.exception(self.moduleName + " - " + errorMsg)
                raise Exception(errorMsg)

            self.logger.debug(self.moduleName + " -- " + "GetDates" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetDates")
            raise
        return returnValue

    def GetYearMonthsToProcess(self, latestValuationDateInAthena):
        '''
        Get list of year months we need to process in the form of yyyy-mm
        '''
        yearMonthsToProcess = []
        availableYearMonthsInTotem = self.GetAvailableYearMonthsInTotem()
        if latestValuationDateInAthena is None:
            yearMonthsToProcess = availableYearMonthsInTotem
        else:
            # Get the year-months to process since the last valuation date in Athena
            dateYrMonthAthena = datetime.datetime.strptime(latestValuationDateInAthena.rsplit('-',1)[0], "%Y-%m")
            for totemYrMonth in availableYearMonthsInTotem:
                datetotemYrMonth = datetime.datetime.strptime(totemYrMonth, "%Y-%m")
                if datetotemYrMonth >= dateYrMonthAthena:
                    yearMonthsToProcess.append(totemYrMonth)
        return yearMonthsToProcess

    def GetFileListForYearMonth(self, dates):
        '''
        gets a list of the files available for the specific date requested
        '''
        fileListArray = []

        url = self.baseUrl +  \
            "/browse/results" + \
            "/" + dates + \
            "/files"
        try:
            self.logger.debug(self.moduleName + " -- " + "GetFileListForYearMonth" + " starting ")
            req = requests.post(url, data=self.logonCreds)
            if req.status_code == 200:
                xmldoc = parseString(req.text)
                itemlist = xmldoc.getElementsByTagName('fileName')
                for elm in itemlist:
                    for cn in elm.childNodes:
                        fileListArray.append(cn.nodeValue.encode())
            else:
                errorMsg = "HTTP Code: " + str(req.status_code) + " Message: " + str(req.text)
                self.logger.exception(self.moduleName + " - " + errorMsg)
                raise Exception(errorMsg)

            self.logger.debug(self.moduleName + " -- " + "GetFileListForYearMonth" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in GetFileListForYearMonth")
            raise
        return fileListArray

    def ProcessGzipFile(self, hstream, fileName):
        '''
        since the file was in a gzip format we need:
        1) save off the file
        2) unzip the contents
        3) convert each file to csv
        '''
        try:
            self.logger.debug(self.moduleName + " -- " + "ProcessGzipFile" + " starting ")
            self.fileUtilities.EmptyFolderContents(self.fileUtilities.gzipFolder)
            localGzipfile = open(self.fileUtilities.gzipFolder + fileName, "wb")
            dmy = localGzipfile.write(hstream) # pylint: disable=unused-variable
            localGzipfile.close()
            
            #  first time it must have a gz extention then we unpack the tar file
            self.fileUtilities.UnpackFile(self.fileUtilities.gzipFolder, self.job["archiveexts"])
            #  get a list of all the xml files
            xmlFileNames = self.fileUtilities.GetListOfFilesRecursively(self.fileUtilities.gzipFolder, '*.xml')
            for xmlFileName in xmlFileNames:
                fShortName = xmlFileName.rsplit('/', 1)
                self.Convert2CSV(xmlFileName, fShortName[1])
            self.logger.debug(self.moduleName + " -- " + "ProcessGzipFile" + " finished ")
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessGzipFile")
            raise

    def GetFile(self, yearMonth, fileName):
        '''
        purpose is to find the file for a specific date and based on the parameters and
        pull that file to the local machine
        '''
        url = self.baseUrl +  \
            "/browse/results"   + \
            "/" + yearMonth + \
            "/files"  + \
            "/" +  fileName

        formvalues = urllib.urlencode(self.logonCreds)
        try:
            self.logger.debug(self.moduleName + " -- " + "GetFile" + " starting : " + fileName)

            fnamesplitarray = fileName.rsplit('.', 1)
            fileExtension = fnamesplitarray[1]

            req = urllib2.Request(url, data=formvalues)
            response = urllib2.urlopen(req)
            hstream = response.read()
            if fileExtension == 'xml':
                self.Convert2CSV(hstream, fileName)
            elif fileExtension == 'gz':
                self.ProcessGzipFile(hstream, fileName)
            else:
                pass
            self.logger.debug(self.moduleName + " -- " + "GetFile" + " finished : " + fileName)
        except Exception as ex:
            if hasattr(ex, 'reason'):
                self.logger.debug('%s - We have failed to reach the server: %s' % (self.moduleName, url))
                self.logger.debug('%s - Reason: code : %s Description: %s ' % (self.moduleName, ex.code, ex.reason)) # pylint: disable=no-member
            elif hasattr(ex, 'code'):
                self.logger.debug('%s - Failed with the following error: %s' % (self.moduleName, url))
                self.logger.debug('%s - Error Code: %s ' % (self.moduleName, ex.code)) # pylint: disable=no-member
            else:
                self.logger.debug('%s - we had an error in getFile' % (self.moduleName))
                self.logger.debug(ex)
            raise
    
    def ProcessTables(self, dbCommonNotUsed, table):
        '''
        the actual process starts here
        '''
        try:
            strDateTodayMinus1 = datetime.datetime.strftime(datetime.date.today() - datetime.timedelta(days=1), "%Y-%m-%d")
            latestValuationDateInAthena = self.GetLatestValuationDateInAthena(table)
            if (latestValuationDateInAthena==strDateTodayMinus1):
                self.logger.debug(self.moduleName + " -- " + "*** Totem data is already up-to-date as of: " + latestValuationDateInAthena + " ***")
                return
            
            self.SetGlobalVariables()
            
            yearMonthsToProcess = self.GetYearMonthsToProcess(latestValuationDateInAthena)
            #yearMonthsToProcess = self.GetYearMonthsToProcess("2017-11-10") # For debugging
            for yearMonth in yearMonthsToProcess:
                self.logger.debug(self.moduleName + " -- " + "Processing Year-Month: " + yearMonth)
                
                strDateToday = datetime.datetime.strftime(datetime.date.today(), "%Y-%m-%d")
                self.fileUtilities.EmptyFolderContents(self.fileUtilities.csvFolder)
                fileListForYearMonth = self.GetFileListForYearMonth(yearMonth)
                for fileName in fileListForYearMonth:
                    self.GetFile(yearMonth, fileName)
                    
                spark = SparkUtilities.GetCreateSparkSession(self.logger)
                df = SparkUtilities.ReadCSVFile(spark, table, self.job["delimiter"], False,
                                                self.fileUtilities.csvFolder, self.logger)
                
                # The data frame contains a number of valuation dates.  Get the distinct valuation dates
                # and create a partition for each valuation date
                distinctValuationDates = sorted(df.select(df.valuationdate).distinct().collect())
                for item in distinctValuationDates:
                    # Process new days only.  Skip today so that we don't get partials.  Otherwise we will have
                    # to delete data from Athena/RedShift to avoid duplicates
                    if item.valuationdate <= latestValuationDateInAthena or item.valuationdate == strDateToday:
                        continue
                    
                    self.logger.debug(self.moduleName + " - Processing Valuation Date: " + item.valuationdate)
                    dfValuationDate = df.filter(df.valuationdate==item.valuationdate)
                    fileBaseName = "ValuationDate-" + item.valuationdate
                    SparkUtilities.SaveParquet(dfValuationDate, self.fileUtilities, fileBaseName)
                    self.UploadFilesCreateAthenaTablesAndSqlScripts(table, self.fileUtilities.parquet, item.valuationdate)
                
                    if "loadToRedshift" in table and table["loadToRedshift"] == "Y":
                        self.LoadDataFromAthenaIntoRedShiftLocalScripts(table)
            self.logger.info(self.moduleName + " - Finished processing.")
        except:
            self.logger.exception(self.moduleName + " - we had an error in ProcessDatabase for " + table["table"])
            raise        

    def Start(self, logger, moduleName, filelocs):
        '''
        main routine for Totem
        '''
        ApplicationBase.Start(self, logger, moduleName, filelocs)
        # At some point this will be part of Start
        ApplicationBase.ProcessInput(self, logger, moduleName, filelocs)            
